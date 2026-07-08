import { useState } from 'react';
import { useQuery, useMutation } from '@tanstack/react-query';
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
  engine: string;
  source: string;
}

type Engine = 'opera_se' | 'opera_3';

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

export function TransactionSnapshot({ engine = 'opera_se' }: { engine?: Engine }) {
  const isOpera3 = engine === 'opera_3';
  const [module, setModule] = useState('');
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [phase, setPhase] = useState<'idle' | 'before_taken' | 'processing'>('idle');
  const [result, setResult] = useState<{ summary: ChangeDetail[]; entry_id: string; tables_changed: number; classification?: any } | null>(null);
  const [expandedEntry, setExpandedEntry] = useState<string | null>(null);
  const [entryDetail, setEntryDetail] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  // Opera 3 only: read via SQL (an Opera 3 SQL-SE company) or FoxPro DBFs.
  // Opera SE is always SQL, so this is ignored on the SE page.
  const [o3ReadMode, setO3ReadMode] = useState<'sql' | 'foxpro'>(() => {
    if (typeof window === 'undefined') return 'sql';
    return (localStorage.getItem('opera3_snapshot_read_mode') as 'sql' | 'foxpro') || 'sql';
  });
  const [compareStem, setCompareStem] = useState<string | null>(null);
  const [compareData, setCompareData] = useState<any>(null);
  const [compareLoading, setCompareLoading] = useState(false);
  const [beforeSummary, setBeforeSummary] = useState<{
    tables_scanned: number;
    tables_per_folder?: Record<string, { matched: number; available_in_folder: number }>;
    effective_filter?: string | null;
    warning?: string | null;
  } | null>(null);
  // Opera 3 FoxPro-over-SMB read: server IP, path within the share, and
  // company code. Persisted across sessions.
  const [serverIp, setServerIp] = useState<string>(() => {
    if (typeof window === 'undefined') return '';
    return localStorage.getItem('opera3_snapshot_server_ip') || '';
  });
  // Path within the SMB share (e.g. "Data" or "Data/P").
  const [opera3Path, setOpera3Path] = useState<string>(() => {
    if (typeof window === 'undefined') return '';
    return localStorage.getItem('opera3_snapshot_data_path') || '';
  });
  // Company-code prefix applied to the company folder (e.g. `Z` → `Z_*`).
  const [opera3Filter, setOpera3Filter] = useState<string>(() => {
    if (typeof window === 'undefined') return '';
    return localStorage.getItem('opera3_snapshot_file_filter') || '';
  });

  // Defaults for the Opera 3 SMB fields (server IP, share, known companies)
  // — from config, so nothing is hard-coded in the UI.
  const { data: o3Defaults } = useQuery({
    queryKey: ['opera3SmbDefaults'],
    queryFn: async () => { const r = await authFetch(`${API}/opera3-smb-defaults`); return r.json(); },
    enabled: isOpera3,
  });
  const o3Companies: { id: string; name: string; code: string; subpath: string }[] = o3Defaults?.companies || [];
  const setLS = (k: string, v: string) => {
    if (typeof window === 'undefined') return;
    if (v.trim()) localStorage.setItem(k, v); else localStorage.removeItem(k);
  };

  const { data: modulesData } = useQuery({
    queryKey: ['snapshotModules'],
    queryFn: async () => { const r = await authFetch(`${API}/modules`); return r.json(); },
  });

  const { data: presetsData, refetch: refetchPresets } = useQuery({
    queryKey: ['snapshotPresets', engine],
    queryFn: async () => { const r = await authFetch(`${API}/presets?engine=${engine}`); return r.json(); },
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
      const params = new URLSearchParams({ module, name, description, engine });
      // FoxPro-over-SMB params only apply to Opera 3 in FoxPro read mode.
      // Opera SE and Opera 3 SQL mode both read via SQL (no server/path).
      if (isOpera3 && o3ReadMode === 'foxpro') {
        const ip = (serverIp || o3Defaults?.server_ip || '').trim();
        if (ip) params.set('server_ip', ip);
        if (opera3Path.trim()) params.set('data_path', opera3Path.trim());
        if (opera3Filter.trim()) params.set('file_filter', opera3Filter.trim());
      }
      const r = await authFetch(`${API}/before?${params}`, { method: 'POST' });
      return r.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        setPhase('before_taken');
        setError(null);
        setBeforeSummary({
          tables_scanned: data.tables_scanned ?? 0,
          tables_per_folder: data.tables_per_folder ?? undefined,
          effective_filter: data.effective_filter ?? null,
          warning: data.warning ?? null,
        });
      } else {
        setError(data.detail || data.error || 'Failed to take before snapshot');
        setBeforeSummary(null);
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
        setModule('');
        setName('');
        setDescription('');
        setBeforeSummary(null);
        refetchLibrary();
        refetchPresets();
      } else {
        setError(data.detail || data.error || 'Failed to take after snapshot');
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

  // Stem extraction: strip the trailing _YYYYMMDD_HHMMSS so entries
  // from different captures of the same transaction group together for
  // the cross-engine comparison.
  const stemOf = (id: string) => id.replace(/_\d{8}_\d{6}$/, '');

  // Normalise an entry's engine (authoritative field from the backend;
  // fall back to legacy `source` for older entries).
  const engineOf = (e: LibraryEntry): Engine =>
    (e.engine as Engine) || (e.source === 'opera3' ? 'opera_3' : 'opera_se');

  // Stems that have captures on BOTH engines — eligible for the
  // cross-engine Compare action (computed across the whole library).
  const stemsByEngine: Record<Engine, Set<string>> = { opera_se: new Set(), opera_3: new Set() };
  library.forEach(e => stemsByEngine[engineOf(e)].add(stemOf(e.id)));
  const comparableStems = new Set(
    [...stemsByEngine.opera_se].filter(s => stemsByEngine.opera_3.has(s)),
  );

  // This page shows ONLY its own engine's captures.
  const filteredLibrary = library.filter(e => engineOf(e) === engine);
  const grouped: Record<string, LibraryEntry[]> = {};
  filteredLibrary.forEach(e => {
    if (!grouped[e.module]) grouped[e.module] = [];
    grouped[e.module].push(e);
  });

  const openCompare = async (id: string) => {
    const stem = stemOf(id);
    setCompareStem(stem);
    setCompareData(null);
    setCompareLoading(true);
    try {
      const r = await authFetch(`${API}/compare?stem=${encodeURIComponent(stem)}`);
      const data = await r.json();
      setCompareData(data);
    } catch (err: any) {
      setCompareData({ success: false, error: err.message });
    } finally {
      setCompareLoading(false);
    }
  };

  const closeCompare = () => {
    setCompareStem(null);
    setCompareData(null);
  };

  return (
    <div className="p-6 max-w-6xl mx-auto space-y-6">
      <PageHeader
        icon={Database}
        title={isOpera3 ? 'Snapshot — Opera 3' : 'Snapshot — Opera SE'}
        subtitle="Capture before/after snapshots of Opera to identify exactly which tables and fields are updated for each transaction type"
      />

      {/* Engine banner + (Opera 3 only) read-mechanism selector */}
      <Card>
        <div className="p-4 space-y-3">
          <div className="flex items-center gap-2">
            <span className={`text-xs uppercase font-semibold px-2 py-0.5 rounded ${isOpera3 ? 'bg-amber-100 text-amber-800' : 'bg-blue-100 text-blue-800'}`}>
              {isOpera3 ? 'Opera 3' : 'Opera SE'}
            </span>
            <span className="text-sm text-gray-600">
              Captures on this page are filed under <b>{isOpera3 ? 'Opera 3' : 'Opera SE'}</b>.
            </span>
          </div>

          {!isOpera3 && (
            <p className="text-xs text-gray-500">
              Reads the active company via <span className="font-semibold text-blue-700">SQL Server</span>. Make sure your active company is an Opera SE company.
            </p>
          )}

          {isOpera3 && (
            <>
              <div className="inline-flex rounded-md border border-gray-300 bg-white text-sm">
                {([
                  ['sql', 'SQL-SE (active company)'],
                  ['foxpro', 'FoxPro server (SMB)'],
                ] as const).map(([k, label], i) => (
                  <button
                    key={k}
                    onClick={() => {
                      setO3ReadMode(k);
                      if (typeof window !== 'undefined') localStorage.setItem('opera3_snapshot_read_mode', k);
                    }}
                    disabled={phase !== 'idle'}
                    className={`px-3 py-1.5 disabled:opacity-50 ${i > 0 ? 'border-l border-gray-300' : ''} ${
                      o3ReadMode === k ? 'bg-amber-600 text-white' : 'hover:bg-gray-50 text-gray-700'
                    } ${i === 0 ? 'rounded-l-md' : 'rounded-r-md'}`}
                  >
                    {label}
                  </button>
                ))}
              </div>

              {o3ReadMode === 'sql' && (
                <p className="text-xs text-gray-500">
                  Reads the active company via <span className="font-semibold text-amber-700">SQL Server</span> (Opera 3 SQL-SE edition). Make sure your active company is an Opera 3 company.
                </p>
              )}

              {o3ReadMode === 'foxpro' && (
                <>
                  {/* Quick-pick a known Opera 3 company — fills path + code */}
                  {o3Companies.length > 0 && (
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Known Opera 3 company</label>
                      <select
                        className="w-full border rounded px-3 py-2 bg-amber-50 text-sm"
                        value=""
                        disabled={phase !== 'idle'}
                        onChange={(e) => {
                          const c = o3Companies.find(x => x.id === e.target.value);
                          if (!c) return;
                          setOpera3Path(c.subpath); setLS('opera3_snapshot_data_path', c.subpath);
                          setOpera3Filter(c.code);  setLS('opera3_snapshot_file_filter', c.code);
                          const ip = (o3Defaults?.server_ip || '').trim();
                          if (ip) { setServerIp(ip); setLS('opera3_snapshot_server_ip', ip); }
                        }}
                      >
                        <option value="">Select a company to auto-fill…</option>
                        {o3Companies.map(c => (
                          <option key={c.id} value={c.id}>{c.name} ({c.code})</option>
                        ))}
                      </select>
                    </div>
                  )}

                  <div>
                    <label className="block text-sm font-medium text-gray-700">Server IP</label>
                    <input
                      type="text"
                      className="w-full border rounded px-3 py-2 font-mono text-sm"
                      placeholder={o3Defaults?.server_ip || '172.17.172.214'}
                      value={serverIp}
                      onChange={(e) => { setServerIp(e.target.value); setLS('opera3_snapshot_server_ip', e.target.value); }}
                      disabled={phase !== 'idle'}
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Connects over SMB — no drive mounting needed. Share
                      {o3Defaults?.share ? <> <code>{o3Defaults.share}</code></> : ' name'} and credentials come from the server config. Leave blank to use the configured default{o3Defaults?.server_ip ? <> (<code>{o3Defaults.server_ip}</code>)</> : ''}.
                    </p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700">Path within share</label>
                    <input
                      type="text"
                      className="w-full border rounded px-3 py-2 font-mono text-sm"
                      placeholder={o3Defaults?.default_subpath || 'Data'}
                      value={opera3Path}
                      onChange={(e) => { setOpera3Path(e.target.value); setLS('opera3_snapshot_data_path', e.target.value); }}
                      disabled={phase !== 'idle'}
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Folder inside the share, e.g. <code>Data</code> (or <code>Data/P</code> for a company in its own sub-folder).
                    </p>
                  </div>

                  <div>
                    <label className="block text-sm font-medium text-gray-700">
                      Company identifier <span className="text-gray-400 font-normal">(e.g. <code>Z</code> — expands to <code>Z_*</code>)</span>
                    </label>
                    <input
                      type="text"
                      className="w-full border rounded px-3 py-2 font-mono text-sm"
                      placeholder="Z   (just the prefix — no underscore, no glob)"
                      value={opera3Filter}
                      onChange={(e) => { setOpera3Filter(e.target.value); setLS('opera3_snapshot_file_filter', e.target.value); }}
                      disabled={phase !== 'idle'}
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      Limits the company-folder scan to those DBFs; System tables are always captured. Very large companies (multi-GB) are refused over SMB — mount those locally instead.
                    </p>
                  </div>
                </>
              )}
            </>
          )}
        </div>
      </Card>

      {/* Capture Panel */}
      <Card>
        <div className="p-5 space-y-4">
          <h2 className="text-lg font-semibold text-gray-800 flex items-center gap-2">
            <Camera className="w-5 h-5 text-blue-600" />
            {phase === 'idle' ? 'New Snapshot' : phase === 'before_taken' ? 'Before Snapshot Taken — Enter Transaction in Opera' : 'Processing...'}
          </h2>

          {phase === 'idle' && (
            <>
              {/* Snapshot checklist — the transaction types still to capture
                  for this engine. Captured types drop off automatically. */}
              {(() => {
                const total = presetsData?.total_presets ?? presets.length;
                const captured = presetsData?.captured ?? 0;
                const pct = total > 0 ? Math.round((captured / total) * 100) : 0;
                return (
                  <div className="border rounded-lg overflow-hidden">
                    <div className="px-3 py-2 bg-gray-50 border-b">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-semibold text-gray-700">
                          Snapshot checklist — {isOpera3 ? 'Opera 3' : 'Opera SE'}
                        </span>
                        <span className="text-xs text-gray-500">{captured} of {total} captured</span>
                      </div>
                      <div className="mt-1.5 h-1.5 w-full bg-gray-200 rounded-full overflow-hidden">
                        <div className="h-full bg-green-500 transition-all" style={{ width: `${pct}%` }} />
                      </div>
                    </div>
                    {presets.length === 0 ? (
                      <div className="px-3 py-4 text-sm text-green-700 bg-green-50">
                        ✓ All {total} transaction types captured — the reference set is complete.
                      </div>
                    ) : (
                      <ul className="divide-y max-h-72 overflow-y-auto">
                        {presets.map((p, i) => {
                          const selected = name === p.name && module === p.module;
                          return (
                            <li key={i}>
                              <button
                                type="button"
                                onClick={() => { setModule(p.module); setName(p.name); setDescription(p.description); }}
                                className={`w-full text-left px-3 py-2 flex items-start gap-2 ${selected ? 'bg-blue-50' : 'hover:bg-gray-50'}`}
                              >
                                <span className={`mt-0.5 w-4 h-4 rounded border flex items-center justify-center flex-shrink-0 ${selected ? 'bg-blue-600 border-blue-600 text-white' : 'border-gray-300'}`}>
                                  {selected && '✓'}
                                </span>
                                <span className="flex-1 min-w-0">
                                  <span className="block text-sm font-medium text-gray-800">{p.name}</span>
                                  <span className="block text-xs text-gray-400">{modules[p.module] || p.module}</span>
                                </span>
                                {selected && <span className="text-xs text-blue-600 font-medium whitespace-nowrap">selected ↓</span>}
                              </button>
                            </li>
                          );
                        })}
                      </ul>
                    )}
                  </div>
                );
              })()}

              <div className="border-t pt-3">
                <p className="text-xs text-gray-500 mb-2">Pick one above, or enter a custom type manually:</p>
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
                {beforeMutation.isPending ? 'Take Before Snapshot' : 'Take Before Snapshot'}
              </button>
              {beforeMutation.isPending && (
                <div className="mt-3 p-3 bg-blue-50 border border-blue-200 rounded-lg flex items-center gap-3">
                  <div className="w-5 h-5 border-2 border-blue-500 border-t-transparent rounded-full animate-spin" />
                  <div>
                    <p className="text-blue-800 font-medium text-sm">Scanning all Opera tables...</p>
                    <p className="text-blue-600 text-xs mt-0.5">
                      Capturing: <b>{module}/{name}</b> {description ? `— ${description}` : ''}
                    </p>
                  </div>
                </div>
              )}
            </>
          )}

          {phase === 'before_taken' && (
            <div className="space-y-3">
              <div className="p-4 bg-green-50 border border-green-200 rounded-lg">
                <p className="text-green-800 font-medium">Before snapshot taken for: <b>{module}/{name}</b></p>
                <p className="text-green-700 text-sm mt-1">Now enter the transaction/record in Opera. When done, click "Take After Snapshot".</p>
              </div>

              {description && (
                <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg">
                  <p className="text-xs font-semibold uppercase tracking-wide text-amber-800 mb-1">Post it exactly like this</p>
                  <p className="text-sm text-amber-900 whitespace-pre-wrap">{description}</p>
                </div>
              )}

              {beforeSummary && beforeSummary.tables_per_folder && (
                <div className={`p-3 rounded-lg border text-sm ${beforeSummary.warning ? 'bg-amber-50 border-amber-300 text-amber-900' : 'bg-blue-50 border-blue-200 text-blue-900'}`}>
                  <div className="font-medium">Captured tables (Opera 3)</div>
                  <ul className="mt-1 ml-4 list-disc">
                    {Object.entries(beforeSummary.tables_per_folder).map(([folder, info]) => (
                      <li key={folder}>
                        <code>{folder}</code>: <b>{info.matched}</b> matched / {info.available_in_folder} present
                        {folder === 'company' && beforeSummary.effective_filter && (
                          <> — filter <code>{beforeSummary.effective_filter}</code></>
                        )}
                      </li>
                    ))}
                  </ul>
                  {beforeSummary.warning && (
                    <p className="mt-2 font-medium flex items-start gap-2">
                      <AlertCircle className="w-4 h-4 flex-shrink-0 mt-0.5" />
                      <span>{beforeSummary.warning}</span>
                    </p>
                  )}
                </div>
              )}
              <div className="flex gap-3">
                <button
                  onClick={() => afterMutation.mutate()}
                  disabled={afterMutation.isPending}
                  className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
                >
                  <Camera className="w-4 h-4" />
                  Take After Snapshot
                </button>
                <button
                  onClick={() => cancelMutation.mutate()}
                  className="px-4 py-2 border border-gray-300 rounded hover:bg-gray-50 flex items-center gap-2"
                >
                  <Square className="w-4 h-4" />
                  Cancel
                </button>
                {afterMutation.isPending && (
                  <div className="flex items-center gap-3 ml-2">
                    <div className="w-5 h-5 border-2 border-green-500 border-t-transparent rounded-full animate-spin" />
                    <span className="text-green-700 text-sm font-medium">Scanning and comparing all tables...</span>
                  </div>
                )}
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
          <div className="flex items-center justify-between gap-3 flex-wrap">
            <h2 className="text-lg font-semibold text-gray-800">
              Transaction Library — {isOpera3 ? 'Opera 3' : 'Opera SE'}
              <span className="text-sm font-normal text-gray-500 ml-2">({filteredLibrary.length})</span>
            </h2>
          </div>

          {comparableStems.size > 0 && (
            <div className="text-xs text-gray-600 bg-purple-50 border border-purple-200 rounded px-3 py-2">
              <strong className="text-purple-800">{comparableStems.size}</strong> transaction(s) have captures on both engines —
              click <span className="font-mono">Compare engines</span> on any matching entry below to see the SE vs Opera 3 diff.
            </div>
          )}

          {Object.keys(grouped).length === 0 && (
            <p className="text-gray-500 text-sm">
              No {isOpera3 ? 'Opera 3' : 'Opera SE'} snapshots recorded yet. Use the tool above to capture your first transaction.
            </p>
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
                        <span
                          className={`text-[10px] font-semibold uppercase tracking-wider px-1.5 py-0.5 rounded ml-2 ${
                            engineOf(entry) === 'opera_3' ? 'bg-amber-100 text-amber-800' : 'bg-blue-100 text-blue-800'
                          }`}
                          title={`Filed under ${engineOf(entry) === 'opera_3' ? 'Opera 3' : 'Opera SE'} · read via ${entry.source === 'opera3' ? 'FoxPro' : 'SQL'}`}
                        >
                          {engineOf(entry) === 'opera_3' ? 'O3' : 'SE'}
                        </span>
                        <span className="text-gray-500 text-sm ml-2">
                          {entry.tables_changed} table(s) • {entry.recorded_at?.split('T')[0]}
                        </span>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      {comparableStems.has(stemOf(entry.id)) && (
                        <button
                          onClick={(e) => { e.stopPropagation(); openCompare(entry.id); }}
                          className="px-2 py-1 text-xs font-medium bg-purple-600 text-white rounded hover:bg-purple-700"
                          title="Compare SE vs Opera 3 captures of this transaction"
                        >
                          Compare engines
                        </button>
                      )}
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

      {compareStem && (
        <div
          className="fixed inset-0 bg-black/40 flex items-start justify-center p-6 z-50 overflow-y-auto"
          onClick={closeCompare}
        >
          <div className="bg-white rounded-lg shadow-xl max-w-5xl w-full my-8" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between p-4 border-b">
              <h3 className="text-lg font-semibold text-gray-800">
                Engine comparison — <span className="font-mono text-base">{compareStem}</span>
              </h3>
              <button onClick={closeCompare} className="text-gray-500 hover:text-gray-800 text-xl px-2">×</button>
            </div>
            <div className="p-5">
              {compareLoading && <p className="text-gray-500">Loading comparison…</p>}
              {!compareLoading && compareData && !compareData.success && (
                <div className="text-amber-800 bg-amber-50 border border-amber-200 rounded p-3 text-sm">
                  {compareData.error || 'Unable to compare.'}
                </div>
              )}
              {!compareLoading && compareData && compareData.success && (
                <div className="space-y-4 text-sm">
                  <div className="grid grid-cols-2 gap-4 text-center">
                    <div className="bg-blue-50 border border-blue-200 rounded p-3">
                      <div className="text-xs uppercase font-semibold text-blue-700">Opera SE</div>
                      <div className="text-2xl font-bold text-blue-900 mt-1">{compareData.summary.se_tables_count}</div>
                      <div className="text-xs text-blue-700">tables touched</div>
                      <div className="text-[10px] text-blue-600 mt-1 font-mono break-all">{compareData.se_entry_id}</div>
                    </div>
                    <div className="bg-amber-50 border border-amber-200 rounded p-3">
                      <div className="text-xs uppercase font-semibold text-amber-700">Opera 3</div>
                      <div className="text-2xl font-bold text-amber-900 mt-1">{compareData.summary.o3_tables_count}</div>
                      <div className="text-xs text-amber-700">tables touched</div>
                      <div className="text-[10px] text-amber-600 mt-1 font-mono break-all">{compareData.o3_entry_id}</div>
                    </div>
                  </div>

                  <div className="grid grid-cols-3 gap-2 text-center text-xs">
                    <div className="bg-green-50 border border-green-200 rounded p-2">
                      <div className="font-semibold text-green-700">Touched by both</div>
                      <div className="text-xl font-bold text-green-900">{compareData.summary.tables_in_both}</div>
                    </div>
                    <div className="bg-blue-50 border border-blue-200 rounded p-2">
                      <div className="font-semibold text-blue-700">SE only</div>
                      <div className="text-xl font-bold text-blue-900">{compareData.summary.tables_se_only}</div>
                    </div>
                    <div className="bg-amber-50 border border-amber-200 rounded p-2">
                      <div className="font-semibold text-amber-700">O3 only</div>
                      <div className="text-xl font-bold text-amber-900">{compareData.summary.tables_o3_only}</div>
                    </div>
                  </div>

                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-left border-b bg-gray-50">
                        <th className="py-1.5 px-2">Table (canonical)</th>
                        <th className="py-1.5 px-2">SE</th>
                        <th className="py-1.5 px-2">O3</th>
                        <th className="py-1.5 px-2">Fields in both</th>
                        <th className="py-1.5 px-2 text-blue-700">SE-only fields</th>
                        <th className="py-1.5 px-2 text-amber-700">O3-only fields</th>
                      </tr>
                    </thead>
                    <tbody>
                      {compareData.tables.map((t: any) => {
                        const rowCls =
                          t.in_se && t.in_o3 ? 'bg-green-50' :
                          t.in_se ? 'bg-blue-50' :
                          'bg-amber-50';
                        return (
                          <tr key={t.canonical} className={`border-b border-gray-200 ${rowCls}`}>
                            <td className="py-1.5 px-2 font-mono font-semibold">{t.canonical}</td>
                            <td className="py-1.5 px-2 font-mono text-blue-700">
                              {t.in_se ? (t.se_orig_names || []).join(', ') : '—'}
                            </td>
                            <td className="py-1.5 px-2 font-mono text-amber-700">
                              {t.in_o3 ? (t.o3_orig_names || []).join(', ') : '—'}
                            </td>
                            <td className="py-1.5 px-2 font-mono">
                              {(t.fields_both || []).join(', ') || (t.in_se && t.in_o3 ? '(none)' : '—')}
                            </td>
                            <td className="py-1.5 px-2 font-mono text-blue-700">
                              {(t.fields_se_only || t.se_fields_modified || []).join(', ') || '—'}
                            </td>
                            <td className="py-1.5 px-2 font-mono text-amber-700">
                              {(t.fields_o3_only || t.o3_fields_modified || []).join(', ') || '—'}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
