import { useState, useEffect, useCallback } from 'react';
import {
  Lock,
  Play,
  Square,
  RefreshCw,
  AlertTriangle,
  Server,
  Clock,
  Users,
  Table,
  Plus,
  X,
  Activity
} from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

interface LockEvent {
  blocked_session: number;
  blocking_session: number;
  blocked_user: string;
  blocking_user: string;
  table_name: string;
  lock_type: string;
  wait_time_ms: number;
  blocked_query: string;
  blocking_query: string;
  timestamp?: string;
}

interface TableStat {
  table_name: string;
  block_count: number;
  total_wait_ms: number;
  avg_wait_ms: number;
}

interface UserStat {
  user: string;
  block_count: number;
  total_wait_ms: number;
  users_blocked: number;
}

interface HourlyStat {
  hour: number;
  event_count: number;
  avg_wait_ms: number;
}

interface LockSummary {
  total_events: number;
  unique_tables: number;
  total_wait_time_ms: number;
  avg_wait_time_ms: number;
  max_wait_time_ms: number;
  most_blocked_tables: TableStat[];
  most_blocking_users: UserStat[];
  hourly_distribution: HourlyStat[];
  recent_events: LockEvent[];
}

interface Monitor {
  name: string;
  is_monitoring: boolean;
}

interface ConnectionForm {
  name: string;
  server: string;
  database: string;
  username: string;
  password: string;
  useWindowsAuth: boolean;
}

export function LockMonitor() {
  const [monitors, setMonitors] = useState<Monitor[]>([]);
  const [selectedMonitor, setSelectedMonitor] = useState<string | null>(null);
  const [currentLocks, setCurrentLocks] = useState<LockEvent[]>([]);
  const [summary, setSummary] = useState<LockSummary | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showConnectForm, setShowConnectForm] = useState(false);
  const [summaryHours, setSummaryHours] = useState(24);
  const [autoRefresh, setAutoRefresh] = useState(false);

  const [connectionForm, setConnectionForm] = useState<ConnectionForm>(() => ({
    name: localStorage.getItem('lockMonitor_name') || '',
    server: localStorage.getItem('lockMonitor_server') || '',
    database: localStorage.getItem('lockMonitor_database') || '',
    username: localStorage.getItem('lockMonitor_username') || '',
    password: '',
    useWindowsAuth: localStorage.getItem('lockMonitor_useWindowsAuth') === 'true'
  }));

  // Persist connection settings
  useEffect(() => {
    if (connectionForm.name) localStorage.setItem('lockMonitor_name', connectionForm.name);
    if (connectionForm.server) localStorage.setItem('lockMonitor_server', connectionForm.server);
    if (connectionForm.database) localStorage.setItem('lockMonitor_database', connectionForm.database);
    if (connectionForm.username) localStorage.setItem('lockMonitor_username', connectionForm.username);
    localStorage.setItem('lockMonitor_useWindowsAuth', String(connectionForm.useWindowsAuth));
  }, [connectionForm.name, connectionForm.server, connectionForm.database, connectionForm.username, connectionForm.useWindowsAuth]);

  const fetchMonitors = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/lock-monitor/list`);
      const data = await res.json();
      if (data.success) {
        setMonitors(data.monitors);
        if (data.monitors.length > 0 && !selectedMonitor) {
          setSelectedMonitor(data.monitors[0].name);
        }
      }
    } catch (err) {
      console.error('Failed to fetch monitors:', err);
    }
  }, [selectedMonitor]);

  const fetchCurrentLocks = useCallback(async () => {
    if (!selectedMonitor) return;
    try {
      const res = await fetch(`${API_BASE}/lock-monitor/${selectedMonitor}/current`);
      const data = await res.json();
      if (data.success) {
        setCurrentLocks(data.events);
      }
    } catch (err) {
      console.error('Failed to fetch current locks:', err);
    }
  }, [selectedMonitor]);

  const fetchSummary = useCallback(async () => {
    if (!selectedMonitor) return;
    setLoading(true);
    try {
      const res = await fetch(`${API_BASE}/lock-monitor/${selectedMonitor}/summary?hours=${summaryHours}`);
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

  // Initial load
  useEffect(() => {
    fetchMonitors();
  }, [fetchMonitors]);

  // Load data when monitor selected
  useEffect(() => {
    if (selectedMonitor) {
      fetchCurrentLocks();
      fetchSummary();
    }
  }, [selectedMonitor, fetchCurrentLocks, fetchSummary]);

  // Auto-refresh
  useEffect(() => {
    if (!autoRefresh || !selectedMonitor) return;

    const interval = setInterval(() => {
      fetchCurrentLocks();
    }, 5000);

    return () => clearInterval(interval);
  }, [autoRefresh, selectedMonitor, fetchCurrentLocks]);

  const handleConnect = async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams({
        name: connectionForm.name,
        server: connectionForm.server,
        database: connectionForm.database
      });
      if (!connectionForm.useWindowsAuth) {
        params.append('username', connectionForm.username);
        params.append('password', connectionForm.password);
      }

      const res = await fetch(`${API_BASE}/lock-monitor/connect?${params}`, { method: 'POST' });
      const data = await res.json();

      if (data.success) {
        setShowConnectForm(false);
        setSelectedMonitor(connectionForm.name);
        fetchMonitors();
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
      const res = await fetch(`${API_BASE}/lock-monitor/${selectedMonitor}/start?poll_interval=5&min_wait_time=1000`, {
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
      const res = await fetch(`${API_BASE}/lock-monitor/${selectedMonitor}/stop`, { method: 'POST' });
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

  const handleRemoveMonitor = async (name: string) => {
    if (!confirm(`Remove monitor "${name}"?`)) return;
    try {
      const res = await fetch(`${API_BASE}/lock-monitor/${name}`, { method: 'DELETE' });
      const data = await res.json();
      if (data.success) {
        if (selectedMonitor === name) {
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

  const selectedMonitorData = monitors.find(m => m.name === selectedMonitor);
  const isMonitoring = selectedMonitorData?.is_monitoring || false;

  const formatDuration = (ms: number) => {
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
          <p className="text-gray-600 mt-1">Monitor SQL Server record locking and conflicts</p>
        </div>
        <button
          onClick={() => setShowConnectForm(true)}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
        >
          <Plus className="h-4 w-4" />
          Add Connection
        </button>
      </div>

      {/* Connection Form Modal */}
      {showConnectForm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 w-full max-w-md">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-lg font-semibold">Connect to SQL Server</h2>
              <button onClick={() => setShowConnectForm(false)} className="text-gray-500 hover:text-gray-700">
                <X className="h-5 w-5" />
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Connection Name</label>
                <input
                  type="text"
                  value={connectionForm.name}
                  onChange={e => setConnectionForm({ ...connectionForm, name: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md"
                  placeholder="e.g., Production Opera"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Server</label>
                <input
                  type="text"
                  value={connectionForm.server}
                  onChange={e => setConnectionForm({ ...connectionForm, server: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md"
                  placeholder="e.g., localhost\\SQLEXPRESS"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Database</label>
                <input
                  type="text"
                  value={connectionForm.database}
                  onChange={e => setConnectionForm({ ...connectionForm, database: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md"
                  placeholder="e.g., Opera3"
                />
              </div>

              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="windowsAuth"
                  checked={connectionForm.useWindowsAuth}
                  onChange={e => setConnectionForm({ ...connectionForm, useWindowsAuth: e.target.checked })}
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
                      onChange={e => setConnectionForm({ ...connectionForm, username: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
                    <input
                      type="password"
                      value={connectionForm.password}
                      onChange={e => setConnectionForm({ ...connectionForm, password: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md"
                    />
                  </div>
                </>
              )}

              {error && (
                <div className="p-3 bg-red-50 border border-red-200 rounded-md text-red-700 text-sm">
                  {error}
                </div>
              )}

              <div className="flex justify-end gap-3 pt-4">
                <button
                  onClick={() => setShowConnectForm(false)}
                  className="px-4 py-2 border border-gray-300 rounded-md hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={handleConnect}
                  disabled={loading || !connectionForm.name || !connectionForm.server || !connectionForm.database}
                  className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400"
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
            <span className="text-sm font-medium text-gray-700">Connections:</span>
            {monitors.map(m => (
              <div
                key={m.name}
                className={`flex items-center gap-2 px-3 py-1.5 rounded-lg cursor-pointer ${
                  selectedMonitor === m.name
                    ? 'bg-blue-100 border-2 border-blue-500'
                    : 'bg-gray-100 border-2 border-transparent hover:bg-gray-200'
                }`}
                onClick={() => setSelectedMonitor(m.name)}
              >
                <Server className="h-4 w-4" />
                <span className="font-medium">{m.name}</span>
                {m.is_monitoring && (
                  <span className="flex items-center gap-1 text-xs text-green-600">
                    <Activity className="h-3 w-3 animate-pulse" />
                    Active
                  </span>
                )}
                <button
                  onClick={e => { e.stopPropagation(); handleRemoveMonitor(m.name); }}
                  className="ml-2 text-gray-400 hover:text-red-500"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* No monitors message */}
      {monitors.length === 0 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 text-center">
          <AlertTriangle className="h-8 w-8 text-yellow-500 mx-auto mb-2" />
          <p className="text-yellow-800 font-medium">No connections configured</p>
          <p className="text-yellow-600 text-sm mt-1">Click "Add Connection" to connect to a SQL Server instance</p>
        </div>
      )}

      {/* Control Panel */}
      {selectedMonitor && (
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
                {isMonitoring ? (
                  <>
                    <Square className="h-4 w-4" />
                    Stop Monitoring
                  </>
                ) : (
                  <>
                    <Play className="h-4 w-4" />
                    Start Monitoring
                  </>
                )}
              </button>

              <button
                onClick={() => { fetchCurrentLocks(); fetchSummary(); }}
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
              <label className="text-sm text-gray-600">Summary period:</label>
              <select
                value={summaryHours}
                onChange={e => setSummaryHours(Number(e.target.value))}
                className="px-3 py-1.5 border border-gray-300 rounded-md text-sm"
              >
                <option value={1}>Last hour</option>
                <option value={6}>Last 6 hours</option>
                <option value={24}>Last 24 hours</option>
                <option value={72}>Last 3 days</option>
                <option value={168}>Last week</option>
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
              Current Blocking ({currentLocks.length})
            </h2>
          </div>
          {currentLocks.length === 0 ? (
            <div className="p-6 text-center text-gray-500">
              <Lock className="h-8 w-8 mx-auto mb-2 text-gray-300" />
              No blocking detected
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="text-left p-3">Blocked User</th>
                    <th className="text-left p-3">Blocking User</th>
                    <th className="text-left p-3">Table</th>
                    <th className="text-left p-3">Lock Type</th>
                    <th className="text-right p-3">Wait Time</th>
                  </tr>
                </thead>
                <tbody>
                  {currentLocks.map((lock, idx) => (
                    <tr key={idx} className="border-t border-gray-100 hover:bg-gray-50">
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
                      <td className="p-3 text-right font-medium text-orange-600">
                        {formatDuration(lock.wait_time_ms)}
                      </td>
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
          <div className="grid grid-cols-4 gap-4">
            <div className="bg-white rounded-lg shadow p-4">
              <div className="text-sm text-gray-500">Total Events</div>
              <div className="text-2xl font-bold">{summary.total_events.toLocaleString()}</div>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <div className="text-sm text-gray-500">Tables Affected</div>
              <div className="text-2xl font-bold">{summary.unique_tables}</div>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <div className="text-sm text-gray-500">Avg Wait Time</div>
              <div className="text-2xl font-bold">{formatDuration(summary.avg_wait_time_ms)}</div>
            </div>
            <div className="bg-white rounded-lg shadow p-4">
              <div className="text-sm text-gray-500">Max Wait Time</div>
              <div className="text-2xl font-bold text-red-600">{formatDuration(summary.max_wait_time_ms)}</div>
            </div>
          </div>

          {/* Most Blocked Tables */}
          <div className="grid grid-cols-2 gap-6">
            <div className="bg-white rounded-lg shadow">
              <div className="p-4 border-b border-gray-200">
                <h3 className="font-semibold flex items-center gap-2">
                  <Table className="h-4 w-4" />
                  Most Blocked Tables
                </h3>
              </div>
              {summary.most_blocked_tables.length === 0 ? (
                <div className="p-4 text-gray-500 text-center">No data</div>
              ) : (
                <div className="divide-y divide-gray-100">
                  {summary.most_blocked_tables.map((t, idx) => (
                    <div key={idx} className="p-3 flex justify-between items-center">
                      <span className="font-mono text-sm">{t.table_name}</span>
                      <div className="text-right">
                        <span className="font-bold">{t.block_count}</span>
                        <span className="text-gray-400 text-xs ml-1">blocks</span>
                        <span className="text-gray-400 text-xs ml-2">({formatDuration(t.avg_wait_ms)} avg)</span>
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
                  Top Blocking Users
                </h3>
              </div>
              {summary.most_blocking_users.length === 0 ? (
                <div className="p-4 text-gray-500 text-center">No data</div>
              ) : (
                <div className="divide-y divide-gray-100">
                  {summary.most_blocking_users.map((u, idx) => (
                    <div key={idx} className="p-3 flex justify-between items-center">
                      <span className="font-medium">{u.user}</span>
                      <div className="text-right">
                        <span className="font-bold text-red-600">{u.block_count}</span>
                        <span className="text-gray-400 text-xs ml-1">blocks</span>
                        <span className="text-gray-400 text-xs ml-2">({u.users_blocked} users affected)</span>
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
                          className={`w-full rounded-t ${height > 0 ? 'bg-blue-500' : 'bg-gray-100'}`}
                          style={{ height: `${Math.max(height, 2)}%` }}
                          title={`${hour}:00 - ${stat?.event_count || 0} events`}
                        />
                        {hour % 4 === 0 && (
                          <span className="text-xs text-gray-400 mt-1">{hour}</span>
                        )}
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
              <h3 className="font-semibold">Recent Lock Events</h3>
            </div>
            {summary.recent_events.length === 0 ? (
              <div className="p-6 text-center text-gray-500">No events recorded</div>
            ) : (
              <div className="overflow-x-auto max-h-96 overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 sticky top-0">
                    <tr>
                      <th className="text-left p-3">Time</th>
                      <th className="text-left p-3">Blocked</th>
                      <th className="text-left p-3">Blocking</th>
                      <th className="text-left p-3">Table</th>
                      <th className="text-right p-3">Wait</th>
                    </tr>
                  </thead>
                  <tbody>
                    {summary.recent_events.map((event, idx) => (
                      <tr key={idx} className="border-t border-gray-100 hover:bg-gray-50">
                        <td className="p-3 text-xs text-gray-500">
                          {event.timestamp ? new Date(event.timestamp).toLocaleString() : '-'}
                        </td>
                        <td className="p-3">{event.blocked_user}</td>
                        <td className="p-3 text-red-600">{event.blocking_user}</td>
                        <td className="p-3 font-mono text-xs">{event.table_name}</td>
                        <td className="p-3 text-right">{formatDuration(event.wait_time_ms)}</td>
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
