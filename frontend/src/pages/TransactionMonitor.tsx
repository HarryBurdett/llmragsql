import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Activity, Server, Play, Square, RefreshCw, Plus, Trash2, CheckCircle,
  XCircle, AlertTriangle, Eye, Settings, Wifi, WifiOff, Zap, Clock
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
  const [newConn, setNewConn] = useState({ name: '', server_host: '', server_port: '1433', database_name: '', username: '', password: '' });
  const [testResult, setTestResult] = useState<any>(null);
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

  // Mutations
  const addConnection = async () => {
    setError(null);
    const res = await authFetch('/api/monitor/connections', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...newConn, server_port: parseInt(newConn.server_port) }),
    });
    const data = await res.json();
    if (data.success) {
      setShowAddConnection(false);
      setNewConn({ name: '', server_host: '', server_port: '1433', database_name: '', username: '', password: '' });
      setSuccess('Connection saved');
      queryClient.invalidateQueries({ queryKey: ['monitor-connections'] });
    } else {
      setError(data.error);
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

      {/* Connections */}
      <Card>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-gray-900">Connections</h2>
          <button
            onClick={() => setShowAddConnection(!showAddConnection)}
            className="px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-1 text-sm"
          >
            <Plus className="w-4 h-4" /> Add Connection
          </button>
        </div>

        {showAddConnection && (
          <div className="mb-4 p-4 bg-gray-50 rounded-lg space-y-3">
            <div className="grid grid-cols-2 gap-3">
              <input placeholder="Connection Name" value={newConn.name} onChange={e => setNewConn({...newConn, name: e.target.value})} className="px-3 py-2 border rounded-lg text-sm" />
              <input placeholder="Server Host" value={newConn.server_host} onChange={e => setNewConn({...newConn, server_host: e.target.value})} className="px-3 py-2 border rounded-lg text-sm" />
              <input placeholder="Database Name" value={newConn.database_name} onChange={e => setNewConn({...newConn, database_name: e.target.value})} className="px-3 py-2 border rounded-lg text-sm" />
              <input placeholder="Port" value={newConn.server_port} onChange={e => setNewConn({...newConn, server_port: e.target.value})} className="px-3 py-2 border rounded-lg text-sm w-24" />
              <input placeholder="Username" value={newConn.username} onChange={e => setNewConn({...newConn, username: e.target.value})} className="px-3 py-2 border rounded-lg text-sm" />
              <input placeholder="Password" type="password" value={newConn.password} onChange={e => setNewConn({...newConn, password: e.target.value})} className="px-3 py-2 border rounded-lg text-sm" />
            </div>
            <div className="flex gap-2">
              <button onClick={addConnection} className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">Save</button>
              <button onClick={() => setShowAddConnection(false)} className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg text-sm hover:bg-gray-300">Cancel</button>
            </div>
          </div>
        )}

        {connections.length === 0 ? (
          <p className="text-sm text-gray-500">No connections configured. Add a connection to start monitoring.</p>
        ) : (
          <div className="space-y-2">
            {connections.map(conn => (
              <div key={conn.id} className="flex items-center justify-between p-3 border rounded-lg">
                <div>
                  <div className="font-medium text-gray-900">{conn.name}</div>
                  <div className="text-xs text-gray-500">{conn.server_host}:{conn.server_port} / {conn.database_name} (user: {conn.username})</div>
                </div>
                <div className="flex gap-2">
                  <button onClick={() => testConnection(conn.id)} className="px-3 py-1.5 bg-gray-100 text-gray-700 rounded text-xs hover:bg-gray-200">Test</button>
                  <button onClick={() => scanNow(conn.id)} disabled={isRunning} className="px-3 py-1.5 bg-amber-100 text-amber-700 rounded text-xs hover:bg-amber-200 disabled:opacity-50" title="Single scan">
                    <Zap className="w-3.5 h-3.5" />
                  </button>
                  {!isRunning ? (
                    <button onClick={() => startMonitor(conn.id)} className="px-3 py-1.5 bg-green-600 text-white rounded text-xs hover:bg-green-700 flex items-center gap-1">
                      <Play className="w-3 h-3" /> Monitor
                    </button>
                  ) : statusData?.connection_id === conn.id ? (
                    <button onClick={stopMonitor} className="px-3 py-1.5 bg-red-600 text-white rounded text-xs hover:bg-red-700 flex items-center gap-1">
                      <Square className="w-3 h-3" /> Stop
                    </button>
                  ) : null}
                  <button onClick={() => deleteConnection(conn.id)} className="px-2 py-1.5 text-red-400 hover:text-red-600">
                    <Trash2 className="w-3.5 h-3.5" />
                  </button>
                </div>
                {testResult?.id === conn.id && (
                  <div className={`absolute right-0 mt-16 mr-4 p-2 rounded text-xs ${testResult.success ? 'bg-green-50 text-green-800' : 'bg-red-50 text-red-800'}`}>
                    {testResult.success ? `Connected — ${testResult.tables_found} tables, ${testResult.opera_users || 0} users` : testResult.error}
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </Card>

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
