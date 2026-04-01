import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Camera, Play, Square, Trash2, Download, ChevronDown, ChevronRight, AlertCircle, Database } from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card } from '../components/ui';

const API = '/api/transaction-snapshot';

interface LibraryEntry {
  id: string;
  module: string;
  module_name: string;
  name: string;
  description: string;
  recorded_at: string;
  tables_changed: number;
  source: string;
}

interface ChangeDetail {
  database: string;
  table: string;
  rows_added: number;
  rows_deleted: number;
  rows_modified: number;
  fields_modified: string[];
  added_rows?: Record<string, any>[];
  modified_rows?: { pk: string; changes: Record<string, { before: any; after: any }> }[];
}

export function TransactionSnapshot() {
  const queryClient = useQueryClient();
  const [module, setModule] = useState('');
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [phase, setPhase] = useState<'idle' | 'before_taken' | 'processing'>('idle');
  const [result, setResult] = useState<{ summary: ChangeDetail[]; entry_id: string; tables_changed: number } | null>(null);
  const [expandedEntry, setExpandedEntry] = useState<string | null>(null);
  const [entryDetail, setEntryDetail] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  const { data: modulesData } = useQuery({
    queryKey: ['snapshotModules'],
    queryFn: async () => { const r = await authFetch(`${API}/modules`); return r.json(); },
  });

  const { data: presetsData } = useQuery({
    queryKey: ['snapshotPresets'],
    queryFn: async () => { const r = await authFetch(`${API}/presets`); return r.json(); },
  });

  const presets: { module: string; name: string; description: string }[] = presetsData?.presets || [];

  const { data: libraryData, refetch: refetchLibrary } = useQuery({
    queryKey: ['snapshotLibrary'],
    queryFn: async () => { const r = await authFetch(`${API}/library`); return r.json(); },
  });

  const modules: Record<string, string> = modulesData?.modules || {};
  const library: LibraryEntry[] = libraryData?.library || [];

  const beforeMutation = useMutation({
    mutationFn: async () => {
      const params = new URLSearchParams({ module, name, description });
      const r = await authFetch(`${API}/before?${params}`, { method: 'POST' });
      return r.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        setPhase('before_taken');
        setError(null);
      } else {
        setError(data.error || 'Failed to take before snapshot');
      }
    },
    onError: (err: any) => setError(err.message),
  });

  const afterMutation = useMutation({
    mutationFn: async () => {
      const r = await authFetch(`${API}/after`, { method: 'POST' });
      return r.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        setPhase('idle');
        setResult(data);
        setError(null);
        refetchLibrary();
      } else {
        setError(data.error || 'Failed to take after snapshot');
      }
    },
    onError: (err: any) => setError(err.message),
  });

  const cancelMutation = useMutation({
    mutationFn: async () => {
      const r = await authFetch(`${API}/cancel`, { method: 'POST' });
      return r.json();
    },
    onSuccess: () => { setPhase('idle'); setError(null); },
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      const r = await authFetch(`${API}/library/${id}`, { method: 'DELETE' });
      return r.json();
    },
    onSuccess: () => refetchLibrary(),
  });

  const loadDetail = async (id: string) => {
    if (expandedEntry === id) {
      setExpandedEntry(null);
      setEntryDetail(null);
      return;
    }
    const r = await authFetch(`${API}/library/${id}`);
    const data = await r.json();
    if (data.success) {
      setEntryDetail(data.entry);
      setExpandedEntry(id);
    }
  };

  const exportMarkdown = async (id: string) => {
    const r = await authFetch(`${API}/export-to-knowledge?entry_id=${id}`, { method: 'POST' });
    const data = await r.json();
    if (data.success) {
      navigator.clipboard.writeText(data.markdown);
      alert('Markdown copied to clipboard — paste into knowledge base');
    }
  };

  // Group library by module
  const grouped: Record<string, LibraryEntry[]> = {};
  library.forEach(e => {
    if (!grouped[e.module]) grouped[e.module] = [];
    grouped[e.module].push(e);
  });

  return (
    <div className="p-6 max-w-6xl mx-auto space-y-6">
      <PageHeader
        icon={Database}
        title="Transaction Snapshot Tool"
        subtitle="Capture before/after snapshots of Opera to identify exactly which tables and fields are updated for each transaction type"
      />

      {/* Capture Panel */}
      <Card>
        <div className="p-5 space-y-4">
          <h2 className="text-lg font-semibold text-gray-800 flex items-center gap-2">
            <Camera className="w-5 h-5 text-blue-600" />
            {phase === 'idle' ? 'New Snapshot' : phase === 'before_taken' ? 'Before Snapshot Taken — Enter Transaction in Opera' : 'Processing...'}
          </h2>

          {phase === 'idle' && (
            <>
              {/* Preset selector */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Quick Select — Preset Transaction Type</label>
                <select className="w-full border rounded px-3 py-2 bg-blue-50"
                  value=""
                  onChange={e => {
                    const preset = presets[parseInt(e.target.value)];
                    if (preset) {
                      setModule(preset.module);
                      setName(preset.name);
                      setDescription(preset.description);
                    }
                  }}
                >
                  <option value="">Select a preset or enter manually below...</option>
                  {presets.map((p, i) => (
                    <option key={i} value={i}>{modules[p.module] || p.module}: {p.name}</option>
                  ))}
                </select>
              </div>

              <div className="border-t pt-3">
                <p className="text-xs text-gray-500 mb-2">Or enter manually:</p>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Module</label>
                  <select className="w-full border rounded px-3 py-2" value={module} onChange={e => setModule(e.target.value)}>
                    <option value="">Select module...</option>
                    {Object.entries(modules).map(([k, v]) => (
                      <option key={k} value={k}>{v}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Transaction/Record Type</label>
                  <input className="w-full border rounded px-3 py-2" placeholder="e.g., Sales Receipt — BACS, New Customer"
                    value={name} onChange={e => setName(e.target.value)} />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Description (optional)</label>
                <textarea className="w-full border rounded px-3 py-2" rows={2}
                  placeholder="Describe what you will enter in Opera..."
                  value={description} onChange={e => setDescription(e.target.value)} />
              </div>
              <button
                onClick={() => beforeMutation.mutate()}
                disabled={!module || !name || beforeMutation.isPending}
                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2"
              >
                <Play className="w-4 h-4" />
                {beforeMutation.isPending ? 'Scanning all tables...' : 'Take Before Snapshot'}
              </button>
            </>
          )}

          {phase === 'before_taken' && (
            <div className="space-y-3">
              <div className="p-4 bg-amber-50 border border-amber-200 rounded-lg">
                <p className="text-amber-800 font-medium">Now enter the transaction/record in Opera</p>
                <p className="text-amber-700 text-sm mt-1">When done, click "Take After Snapshot" to capture the changes.</p>
              </div>
              <div className="flex gap-3">
                <button
                  onClick={() => afterMutation.mutate()}
                  disabled={afterMutation.isPending}
                  className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
                >
                  <Camera className="w-4 h-4" />
                  {afterMutation.isPending ? 'Scanning and comparing...' : 'Take After Snapshot'}
                </button>
                <button
                  onClick={() => cancelMutation.mutate()}
                  className="px-4 py-2 border border-gray-300 rounded hover:bg-gray-50 flex items-center gap-2"
                >
                  <Square className="w-4 h-4" />
                  Cancel
                </button>
              </div>
            </div>
          )}

          {error && (
            <div className="p-3 bg-red-50 border border-red-200 rounded text-red-800 text-sm flex items-center gap-2">
              <AlertCircle className="w-4 h-4" />
              {error}
            </div>
          )}

          {result && (
            <div className="p-4 bg-green-50 border border-green-200 rounded-lg space-y-3">
              <p className="text-green-800 font-medium">Snapshot captured — {result.tables_changed} table(s) changed</p>

              {/* Auto-classification */}
              {result.classification && (
                <div className="p-3 bg-white border border-green-200 rounded space-y-2">
                  <p className="font-semibold text-gray-800">
                    Auto-detected: {result.classification.precise_definition || result.classification.auto_detected_type}
                  </p>
                  {result.classification.posting_characteristics?.length > 0 && (
                    <ul className="text-xs text-gray-600 space-y-0.5 ml-2">
                      {result.classification.posting_characteristics.map((c: string, i: number) => (
                        <li key={i}>{c}</li>
                      ))}
                    </ul>
                  )}
                  {result.classification.balance_updates?.length > 0 && (
                    <div className="text-xs text-gray-600">
                      <span className="font-medium">Balance updates:</span> {result.classification.balance_updates.join(', ')}
                    </div>
                  )}
                  {result.classification.transfer_files?.length > 0 && (
                    <div className="text-xs text-gray-600">
                      <span className="font-medium">Transfer files:</span> {result.classification.transfer_files.join(', ')}
                    </div>
                  )}
                  {result.classification.vat_tracking && (
                    <div className="text-xs text-amber-700 font-medium">VAT tracking: zvtran/nvat records created</div>
                  )}
                  {Object.keys(result.classification.amount_conventions || {}).length > 0 && (
                    <div className="text-xs text-gray-600">
                      <span className="font-medium">Amounts:</span>
                      {Object.entries(result.classification.amount_conventions).map(([k, v]: [string, any]) => (
                        <span key={k} className="ml-2">{k}: {v}</span>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {/* Table changes */}
              <div className="text-sm text-green-700 space-y-1">
                {result.summary.map((s: any, i: number) => (
                  <div key={i} className="flex items-center gap-2">
                    <span className="font-mono text-xs bg-green-100 px-1 rounded">{s.database}.{s.table}</span>
                    <span>+{s.rows_added} added, {s.rows_modified} modified</span>
                    {s.fields_modified.length > 0 && (
                      <span className="text-green-600 text-xs">({s.fields_modified.join(', ')})</span>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </Card>

      {/* Library */}
      <Card>
        <div className="p-5 space-y-4">
          <h2 className="text-lg font-semibold text-gray-800">Transaction Library</h2>

          {Object.keys(grouped).length === 0 && (
            <p className="text-gray-500 text-sm">No snapshots recorded yet. Use the tool above to capture your first transaction.</p>
          )}

          {Object.entries(grouped).sort((a, b) => a[0].localeCompare(b[0])).map(([mod, entries]) => (
            <div key={mod} className="space-y-2">
              <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide">
                {modules[mod] || mod}
              </h3>
              {entries.map(entry => (
                <div key={entry.id} className="border rounded">
                  <div
                    className="flex items-center justify-between p-3 cursor-pointer hover:bg-gray-50"
                    onClick={() => loadDetail(entry.id)}
                  >
                    <div className="flex items-center gap-3">
                      {expandedEntry === entry.id ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
                      <div>
                        <span className="font-medium">{entry.name}</span>
                        <span className="text-gray-500 text-sm ml-2">
                          {entry.tables_changed} table(s) • {entry.source} • {entry.recorded_at?.split('T')[0]}
                        </span>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <button onClick={(e) => { e.stopPropagation(); exportMarkdown(entry.id); }}
                        className="p-1 text-blue-600 hover:bg-blue-50 rounded" title="Copy as markdown">
                        <Download className="w-4 h-4" />
                      </button>
                      <button onClick={(e) => { e.stopPropagation(); if (confirm('Delete this entry?')) deleteMutation.mutate(entry.id); }}
                        className="p-1 text-red-600 hover:bg-red-50 rounded" title="Delete">
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  </div>

                  {expandedEntry === entry.id && entryDetail && (
                    <div className="p-4 border-t bg-gray-50 space-y-3">
                      {entryDetail.description && (
                        <p className="text-sm text-gray-600">{entryDetail.description}</p>
                      )}
                      <table className="w-full text-sm">
                        <thead>
                          <tr className="text-left border-b">
                            <th className="py-1 pr-2">Database</th>
                            <th className="py-1 pr-2">Table</th>
                            <th className="py-1 pr-2">Added</th>
                            <th className="py-1 pr-2">Modified</th>
                            <th className="py-1">Fields Changed</th>
                          </tr>
                        </thead>
                        <tbody>
                          {entryDetail.changes?.map((c: any, i: number) => (
                            <tr key={i} className="border-b border-gray-200">
                              <td className="py-1 pr-2 font-mono text-xs">{c.database}</td>
                              <td className="py-1 pr-2 font-mono text-xs font-semibold">{c.table}</td>
                              <td className="py-1 pr-2">{c.rows_added > 0 ? `+${c.rows_added}` : ''}</td>
                              <td className="py-1 pr-2">{c.modified_rows?.length > 0 ? c.modified_rows.length : ''}</td>
                              <td className="py-1 text-xs text-gray-600">{c.modified_fields?.join(', ')}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>

                      {/* Show added row details */}
                      {entryDetail.changes?.filter((c: any) => c.added_rows?.length > 0).map((c: any, i: number) => (
                        <details key={`add-${i}`} className="text-xs">
                          <summary className="cursor-pointer text-blue-600 font-medium">{c.table} — added rows</summary>
                          <pre className="bg-white p-2 rounded border mt-1 overflow-x-auto max-h-60">
                            {JSON.stringify(c.added_rows, null, 2)}
                          </pre>
                        </details>
                      ))}

                      {/* Show modified row details */}
                      {entryDetail.changes?.filter((c: any) => c.modified_rows?.length > 0).map((c: any, i: number) => (
                        <details key={`mod-${i}`} className="text-xs">
                          <summary className="cursor-pointer text-amber-600 font-medium">{c.table} — modified fields</summary>
                          <pre className="bg-white p-2 rounded border mt-1 overflow-x-auto max-h-60">
                            {JSON.stringify(c.modified_rows, null, 2)}
                          </pre>
                        </details>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}
