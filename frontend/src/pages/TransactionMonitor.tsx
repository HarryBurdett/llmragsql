import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Activity, Server, Play, Square, RefreshCw, Plus, Trash2, CheckCircle,
  XCircle, AlertTriangle, Eye, Wifi, WifiOff, Zap, Database, TestTube,
  ChevronDown, ChevronRight, Pencil, Save
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, Alert } from '../components/ui';

interface Connection {
  id: number;
  name: string;
  server_host: string;
  server_port: number;
  database_name: string;
  username: string;
  is_active: number;
  last_connected_at: string | null;
}

interface CoverageItem {
  category: string;
  type: string;
  captured: number;
  status: string;
}

interface CapturedTransaction {
  id: number;
  transaction_type: string;
  classification: string;
  is_verified: number;
  is_suspicious: number;
  suspicious_reason: string;
  input_by: string;
  tables: string[];
  rows: Record<string, any[]>;
  captured_at: string;
}

export default function TransactionMonitor() {
  const queryClient = useQueryClient();
  const [showAddConnection, setShowAddConnection] = useState(false);
  const [newConnName, setNewConnName] = useState('');
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [editingNameId, setEditingNameId] = useState<number | null>(null);
  const [editName, setEditName] = useState('');
  const [form, setForm] = useState<{ server_host: string; server_port: string; database_name: string; username: string; password: string } | null>(null);
  const [testResult, setTestResult] = useState<{ id: number; success: boolean; message?: string; error?: string; tables_found?: number; opera_users?: number } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [selectedTxn, setSelectedTxn] = useState<CapturedTransaction | null>(null);

  // Queries
  const { data: connectionsData } = useQuery({
    queryKey: ['monitor-connections'],
    queryFn: async () => { const r = await authFetch('/api/monitor/connections'); return r.json(); },
  });

  const { data: statusData, refetch: refetchStatus } = useQuery({
    queryKey: ['monitor-status'],
    queryFn: async () => { const r = await authFetch('/api/monitor/status'); return r.json(); },
    refetchInterval: 3000,
  });

  const { data: coverageData, refetch: refetchCoverage } = useQuery({
    queryKey: ['monitor-coverage'],
    queryFn: async () => { const r = await authFetch('/api/monitor/coverage'); return r.json(); },
    refetchInterval: 10000,
  });

  const { data: txnsData, refetch: refetchTxns } = useQuery({
    queryKey: ['monitor-transactions'],
    queryFn: async () => { const r = await authFetch('/api/monitor/transactions?limit=50'); return r.json(); },
    refetchInterval: 10000,
  });

  const connections: Connection[] = connectionsData?.connections || [];
  const isRunning = statusData?.is_running || false;
  const coverage = coverageData?.checklist || [];
  const transactions: CapturedTransaction[] = txnsData?.transactions || [];

  // Load form when expanding a connection
  useEffect(() => {
    if (expandedId) {
      const conn = connections.find(c => c.id === expandedId);
      if (conn) setForm({
        server_host: conn.server_host || '',
        server_port: String(conn.server_port || 1433),
        database_name: conn.database_name || '',
        username: conn.username || '',
        password: '',
      });
    } else {
      setForm(null);
    }
  }, [expandedId]); // eslint-disable-line react-hooks/exhaustive-deps

  const updateForm = (updates: Partial<NonNullable<typeof form>>) => {
    if (form) setForm({ ...form, ...updates });
  };

  // Mutations
  const addConnection = async () => {
    if (!newConnName.trim()) return;
    setError(null);
    const res = await authFetch('/api/monitor/connections', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: newConnName.trim(), server_host: '', database_name: '', username: '', password: '', server_port: 1433 }),
    });
    const data = await res.json();
    if (data.success) {
      setShowAddConnection(false);
      setNewConnName('');
      setSuccess(`"${newConnName.trim()}" created`);
      queryClient.invalidateQueries({ queryKey: ['monitor-connections'] });
      setTimeout(() => setExpandedId(data.connection_id), 100);
    } else {
      setError(data.error);
    }
  };

  const saveConnection = async (conn: Connection) => {
    if (!form) return;
    setError(null);
    const payload: any = {
      server_host: form.server_host,
      server_port: parseInt(form.server_port) || 1433,
      database_name: form.database_name,
      username: form.username,
    };
    if (form.password) payload.password = form.password;
    const res = await authFetch(`/api/monitor/connections/${conn.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (data.success) {
      setSuccess(`"${conn.name}" settings saved`);
      queryClient.invalidateQueries({ queryKey: ['monitor-connections'] });
    } else {
      setError(data.error);
    }
  };

  const renameConnection = async (conn: Connection) => {
    if (!editName.trim() || editName.trim() === conn.name) { setEditingNameId(null); return; }
    const res = await authFetch(`/api/monitor/connections/${conn.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: editName.trim() }),
    });
    const data = await res.json();
    if (data.success) {
      setEditingNameId(null);
      setSuccess('Renamed');
      queryClient.invalidateQueries({ queryKey: ['monitor-connections'] });
    }
  };

  const testConnection = async (id: number) => {
    setTestResult(null);
    const res = await authFetch(`/api/monitor/connections/${id}/test`, { method: 'POST' });
    const data = await res.json();
    setTestResult({ id, ...data });
  };

  const startMonitor = async (connectionId: number) => {
    setError(null);
    const res = await authFetch('/api/monitor/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ connection_id: connectionId }),
    });
    const data = await res.json();
    if (data.success) {
      setSuccess('Monitor started');
      refetchStatus();
    } else {
      setError(data.error);
    }
  };

  const stopMonitor = async () => {
    const res = await authFetch('/api/monitor/stop', { method: 'POST' });
    const data = await res.json();
    if (data.success) {
      setSuccess('Monitor stopped');
      refetchStatus();
    }
  };

  const deleteConnection = async (id: number) => {
    const res = await authFetch(`/api/monitor/connections/${id}`, { method: 'DELETE' });
    const data = await res.json();
    if (data.success) {
      if (expandedId === id) setExpandedId(null);
      queryClient.invalidateQueries({ queryKey: ['monitor-connections'] });
    }
  };

  const scanNow = async (connectionId: number) => {
    setError(null);
    const res = await authFetch('/api/monitor/scan-now', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ connection_id: connectionId }),
    });
    const data = await res.json();
    if (data.success) {
      setSuccess(data.message);
      refetchCoverage();
      refetchTxns();
    } else {
      setError(data.error);
    }
  };

  // Group coverage by category
  const coverageByCategory: Record<string, CoverageItem[]> = {};
  coverage.forEach((item: CoverageItem) => {
    if (!coverageByCategory[item.category]) coverageByCategory[item.category] = [];
    coverageByCategory[item.category].push(item);
  });

  return (
    <div className="space-y-6">
      <PageHeader title="Transaction Monitor" subtitle="Passively monitor Opera systems to capture transaction patterns" icon={Activity} />

      {error && <Alert variant="error" onDismiss={() => setError(null)}>{error}</Alert>}
      {success && <Alert variant="success" onDismiss={() => setSuccess(null)}>{success}</Alert>}

      {/* Monitor Status Bar */}
      <Card className={isRunning ? 'border-green-300 bg-green-50' : ''}>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            {isRunning ? (
              <Wifi className="w-5 h-5 text-green-600 animate-pulse" />
            ) : (
              <WifiOff className="w-5 h-5 text-gray-400" />
            )}
            <div>
              <div className="font-medium text-gray-900">
                {isRunning ? `Monitoring: ${statusData?.connection_name || 'Active'}` : 'Monitor Idle'}
              </div>
              {isRunning && (
                <div className="text-xs text-gray-500 flex gap-4 mt-0.5">
                  <span>Captured: {statusData?.total_captured || 0}</span>
                  <span>Verified: {statusData?.verified || 0}</span>
                  <span>Suspicious: {statusData?.suspicious || 0}</span>
                  <span>Polls: {statusData?.polls || 0}</span>
                  {statusData?.last_activity && <span>Last: {new Date(statusData.last_activity).toLocaleTimeString()}</span>}
                </div>
              )}
            </div>
          </div>
          {isRunning && (
            <button onClick={stopMonitor} className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 flex items-center gap-2">
              <Square className="w-4 h-4" /> Stop
            </button>
          )}
        </div>
      </Card>

      {/* Connections — mirrors Installations page pattern */}
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-gray-900">Monitor Connections</h2>
        {!showAddConnection && (
          <button
            onClick={() => setShowAddConnection(true)}
            className="px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-1 text-sm"
          >
            <Plus className="w-4 h-4" /> Add Connection
          </button>
        )}
      </div>

      {showAddConnection && (
        <Card className="mb-3">
          <div className="flex items-center gap-3">
            <input
              type="text"
              className="input flex-1"
              placeholder="Connection name (e.g. Acme Ltd — Live)"
              value={newConnName}
              onChange={e => setNewConnName(e.target.value)}
              autoFocus
              onKeyDown={e => { if (e.key === 'Enter') addConnection(); if (e.key === 'Escape') setShowAddConnection(false); }}
            />
            <button onClick={addConnection} className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Create</button>
            <button onClick={() => setShowAddConnection(false)} className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg text-sm hover:bg-gray-300">Cancel</button>
          </div>
        </Card>
      )}

      {connections.length === 0 && !showAddConnection ? (
        <Card>
          <p className="text-sm text-gray-500 py-4 text-center">No monitor connections configured. Add a connection to start capturing transaction patterns.</p>
        </Card>
      ) : (
        <div className="space-y-3">
          {connections.map(conn => {
            const isExpanded = expandedId === conn.id;
            const isMonitoring = isRunning && statusData?.connection_id === conn.id;
            const isEditingName = editingNameId === conn.id;

            return (
              <Card key={conn.id} className={isMonitoring ? 'ring-2 ring-green-200' : ''}>
                {/* Header row */}
                <div className="flex items-center justify-between">
                  <div
                    className="flex items-center gap-3 min-w-0 flex-1 cursor-pointer"
                    onClick={() => setExpandedId(isExpanded ? null : conn.id)}
                  >
                    {isExpanded
                      ? <ChevronDown className="h-4 w-4 text-gray-400 flex-shrink-0" />
                      : <ChevronRight className="h-4 w-4 text-gray-400 flex-shrink-0" />
                    }
                    <span className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${isMonitoring ? 'bg-green-500 animate-pulse' : 'bg-gray-300'}`} />
                    {isEditingName ? (
                      <input
                        type="text"
                        className="input py-1 px-2 text-sm w-56"
                        value={editName}
                        onChange={e => setEditName(e.target.value)}
                        autoFocus
                        onClick={e => e.stopPropagation()}
                        onBlur={() => renameConnection(conn)}
                        onKeyDown={e => { if (e.key === 'Enter') renameConnection(conn); if (e.key === 'Escape') setEditingNameId(null); }}
                      />
                    ) : (
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-semibold text-gray-900">{conn.name}</span>
                          {isMonitoring && (
                            <span className="text-xs text-green-700 font-medium bg-green-100 px-1.5 py-0.5 rounded">Monitoring</span>
                          )}
                        </div>
                        <div className="flex items-center gap-2 mt-0.5 text-xs text-gray-400">
                          {conn.server_host && <span>{conn.server_host}:{conn.server_port}</span>}
                          {conn.server_host && conn.database_name && <span className="text-gray-300">|</span>}
                          {conn.database_name && <span>{conn.database_name}</span>}
                          {!conn.server_host && !conn.database_name && <span className="text-amber-500">Not configured</span>}
                        </div>
                      </div>
                    )}
                  </div>

                  <div className="flex items-center gap-1 flex-shrink-0 ml-3">
                    {!isRunning && conn.server_host && (
                      <button
                        onClick={() => startMonitor(conn.id)}
                        className="px-2.5 py-1 text-xs font-medium text-green-600 bg-green-50 rounded-md hover:bg-green-100 transition-colors flex items-center gap-1"
                      >
                        <Play className="w-3 h-3" /> Monitor
                      </button>
                    )}
                    {isMonitoring && (
                      <button
                        onClick={stopMonitor}
                        className="px-2.5 py-1 text-xs font-medium text-red-600 bg-red-50 rounded-md hover:bg-red-100 transition-colors flex items-center gap-1"
                      >
                        <Square className="w-3 h-3" /> Stop
                      </button>
                    )}
                    {!isRunning && conn.server_host && (
                      <button
                        onClick={() => scanNow(conn.id)}
                        title="Single scan"
                        className="p-1.5 rounded text-gray-400 hover:text-amber-500 hover:bg-amber-50 transition-colors"
                      >
                        <Zap className="h-3.5 w-3.5" />
                      </button>
                    )}
                    <button
                      onClick={() => { setEditingNameId(conn.id); setEditName(conn.name); }}
                      title="Rename"
                      className="p-1.5 rounded text-gray-400 hover:text-blue-600 hover:bg-blue-50 transition-colors"
                    >
                      <Pencil className="h-3.5 w-3.5" />
                    </button>
                    {!isMonitoring && connections.length > 0 && (
                      <button
                        onClick={() => deleteConnection(conn.id)}
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
                    <div className="space-y-3">
                      <h4 className="text-sm font-medium text-gray-700 flex items-center gap-1.5">
                        <Database className="h-4 w-4 text-gray-400" />
                        SQL Server Connection
                      </h4>
                      <p className="text-xs text-gray-500">Read-only connection to the target Opera database. Requires a SQL login with db_datareader role.</p>
                      <div className="grid grid-cols-3 gap-3">
                        <div className="col-span-2">
                          <label className="label">Server</label>
                          <input
                            type="text"
                            className="input"
                            placeholder="IP address or hostname"
                            value={form.server_host}
                            onChange={e => updateForm({ server_host: e.target.value })}
                          />
                        </div>
                        <div>
                          <label className="label">Port</label>
                          <input
                            type="text"
                            className="input"
                            placeholder="1433"
                            value={form.server_port}
                            onChange={e => updateForm({ server_port: e.target.value })}
                          />
                        </div>
                      </div>
                      <div>
                        <label className="label">Database Name</label>
                        <input
                          type="text"
                          className="input"
                          placeholder="Opera3SECompany00X"
                          value={form.database_name}
                          onChange={e => updateForm({ database_name: e.target.value })}
                        />
                      </div>
                      <div className="grid grid-cols-2 gap-3">
                        <div>
                          <label className="label">Username</label>
                          <input
                            type="text"
                            className="input"
                            placeholder="SQL login username"
                            value={form.username}
                            onChange={e => updateForm({ username: e.target.value })}
                          />
                        </div>
                        <div>
                          <label className="label">Password</label>
                          <input
                            type="password"
                            className="input"
                            placeholder="Password"
                            value={form.password}
                            onChange={e => updateForm({ password: e.target.value })}
                          />
                        </div>
                      </div>
                    </div>

                    {/* Test result */}
                    {testResult?.id === conn.id && (
                      <div className={`p-3 rounded-lg text-sm ${testResult.success ? 'bg-green-50 text-green-800 border border-green-200' : 'bg-red-50 text-red-800 border border-red-200'}`}>
                        {testResult.success ? (
                          <div className="flex items-center gap-2">
                            <CheckCircle className="w-4 h-4 text-green-600" />
                            <span>{testResult.message} — {testResult.opera_users || 0} Opera users found</span>
                          </div>
                        ) : (
                          <div className="flex items-center gap-2">
                            <XCircle className="w-4 h-4 text-red-600" />
                            <span>{testResult.error}</span>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Action buttons */}
                    <div className="flex items-center gap-3 pt-2">
                      <button
                        onClick={() => saveConnection(conn)}
                        className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700 flex items-center gap-2"
                      >
                        <Save className="w-4 h-4" /> Save Settings
                      </button>
                      <button
                        onClick={() => testConnection(conn.id)}
                        className="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg text-sm hover:bg-gray-200 flex items-center gap-2"
                      >
                        <TestTube className="w-4 h-4" /> Test Connection
                      </button>
                    </div>
                  </div>
                )}
              </Card>
            );
          })}
        </div>
      )}

      {/* Coverage Dashboard */}
      <Card>
        <div className="flex items-center justify-between mb-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Coverage</h2>
            <p className="text-sm text-gray-500">
              {coverageData?.captured_types || 0} of {coverageData?.total_types || 0} types captured ({coverageData?.coverage_pct || 0}%)
            </p>
          </div>
          <button onClick={() => refetchCoverage()} className="text-gray-400 hover:text-gray-600">
            <RefreshCw className="w-4 h-4" />
          </button>
        </div>

        {/* Progress bar */}
        <div className="w-full bg-gray-200 rounded-full h-2 mb-4">
          <div className="bg-green-600 h-2 rounded-full transition-all" style={{ width: `${coverageData?.coverage_pct || 0}%` }} />
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {Object.entries(coverageByCategory).map(([category, items]) => (
            <div key={category} className="border rounded-lg p-3">
              <h3 className="font-medium text-sm text-gray-700 mb-2">{category}</h3>
              <div className="space-y-1">
                {items.map(item => (
                  <div key={item.type} className="flex items-center justify-between text-sm">
                    <div className="flex items-center gap-2">
                      {item.captured > 0 ? (
                        <CheckCircle className="w-3.5 h-3.5 text-green-600" />
                      ) : (
                        <XCircle className="w-3.5 h-3.5 text-gray-300" />
                      )}
                      <span className={item.captured > 0 ? 'text-gray-900' : 'text-gray-400'}>{item.type}</span>
                    </div>
                    {item.captured > 0 && (
                      <span className="text-xs text-green-700 bg-green-50 px-1.5 rounded">{item.captured}</span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </Card>

      {/* Recent Captures */}
      <Card>
        <h2 className="text-lg font-semibold text-gray-900 mb-4">Recent Captures</h2>
        {transactions.length === 0 ? (
          <p className="text-sm text-gray-500">No transactions captured yet. Start monitoring a connection to begin.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Time</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Type</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Input By</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Tables</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Status</th>
                  <th className="px-3 py-2"></th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-100">
                {transactions.map(txn => (
                  <tr key={txn.id} className={txn.is_suspicious ? 'bg-amber-50' : ''}>
                    <td className="px-3 py-2 text-xs text-gray-500 whitespace-nowrap">
                      {txn.captured_at ? new Date(txn.captured_at).toLocaleString('en-GB', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : ''}
                    </td>
                    <td className="px-3 py-2 text-sm font-medium text-gray-900">{txn.transaction_type || 'Unknown'}</td>
                    <td className="px-3 py-2 text-sm text-gray-600">{txn.input_by || '-'}</td>
                    <td className="px-3 py-2 text-xs text-gray-500">{Array.isArray(txn.tables) ? txn.tables.join(', ') : ''}</td>
                    <td className="px-3 py-2">
                      {txn.is_verified ? (
                        <span className="inline-flex items-center gap-1 text-xs text-green-700 bg-green-50 px-2 py-0.5 rounded">
                          <CheckCircle className="w-3 h-3" /> Verified
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 text-xs text-amber-700 bg-amber-50 px-2 py-0.5 rounded" title={txn.suspicious_reason}>
                          <AlertTriangle className="w-3 h-3" /> Suspicious
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2">
                      <button
                        onClick={() => setSelectedTxn(selectedTxn?.id === txn.id ? null : txn)}
                        className="text-blue-600 hover:text-blue-800"
                      >
                        <Eye className="w-4 h-4" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {/* Transaction Detail Modal */}
      {selectedTxn && (
        <Card className="border-blue-200">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold text-gray-900">{selectedTxn.transaction_type} — Detail</h3>
            <button onClick={() => setSelectedTxn(null)} className="text-gray-400 hover:text-gray-600 text-lg">&times;</button>
          </div>
          <div className="text-sm space-y-2 mb-3">
            <div><strong>Input By:</strong> {selectedTxn.input_by || 'N/A'}</div>
            <div><strong>Captured:</strong> {selectedTxn.captured_at}</div>
            <div><strong>Status:</strong> {selectedTxn.is_verified ? 'Verified Opera user' : `Suspicious — ${selectedTxn.suspicious_reason}`}</div>
            <div><strong>Tables:</strong> {Array.isArray(selectedTxn.tables) ? selectedTxn.tables.join(', ') : ''}</div>
          </div>
          {selectedTxn.rows && typeof selectedTxn.rows === 'object' && Object.entries(selectedTxn.rows).map(([table, rows]) => (
            <div key={table} className="mb-3">
              <h4 className="text-sm font-medium text-gray-700 mb-1">{table} ({Array.isArray(rows) ? rows.length : 0} row{Array.isArray(rows) && rows.length !== 1 ? 's' : ''})</h4>
              {Array.isArray(rows) && rows.length > 0 && (
                <div className="overflow-x-auto">
                  <table className="min-w-full text-xs border">
                    <thead className="bg-gray-100">
                      <tr>
                        {Object.keys(rows[0]).filter(k => rows[0][k] !== null && rows[0][k] !== '').map(k => (
                          <th key={k} className="px-2 py-1 text-left font-medium text-gray-600 border-b">{k}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row: any, i: number) => (
                        <tr key={i}>
                          {Object.entries(row).filter(([, v]) => v !== null && v !== '').map(([k, v]) => (
                            <td key={k} className="px-2 py-1 border-b text-gray-700 whitespace-nowrap">{String(v)}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          ))}
        </Card>
      )}
    </div>
  );
}
