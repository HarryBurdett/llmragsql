import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  ArrowLeft,
  Building,
  Mail,
  Phone,
  User,
  FileText,
  Activity,
  AlertTriangle,
  CheckCircle,
  Clock,
  ArrowDownLeft,
  ArrowUpRight,
} from 'lucide-react';
import { authFetch } from '../api/client';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface SupplierConfig {
  account_code: string;
  name: string;
  balance: number | null;
  reconciliation_active: boolean | number;
  auto_respond: boolean | number;
  never_communicate: boolean | number;
  last_statement_date: string | null;
  payment_terms_days: number | null;
  last_synced: string | null;
}

interface OperaContact {
  name: string;
  role: string;
  email: string;
  phone: string;
  mobile: string;
}

interface StatementHistoryEntry {
  id: number;
  statement_date: string | null;
  received_date: string | null;
  status: string;
  line_count: number | null;
  matched_count: number | null;
  query_count: number | null;
  closing_balance: number | null;
}

interface SupplierDetailResponse {
  success: boolean;
  account: string;
  config: SupplierConfig | null;
  opera_contact: OperaContact | null;
  statement_history: StatementHistoryEntry[];
  balance: number | null;
  error?: string;
}

interface Communication {
  id: number;
  supplier_code: string;
  direction: string;
  comm_type: string;
  email_subject: string | null;
  email_from: string | null;
  email_to: string | null;
  summary: string | null;
  created_at: string | null;
}

interface CommunicationsResponse {
  success: boolean;
  communications: Communication[];
  error?: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmtDate(d: string | null | undefined): string {
  if (!d) return '';
  const s = d.replace('T', ' ').split(' ')[0];
  const parts = s.split('-');
  if (parts.length === 3 && parts[0].length === 4) {
    return `${parts[2]}/${parts[1]}/${parts[0]}`;
  }
  return d;
}

function fmtDateTime(d: string | null | undefined): string {
  if (!d) return '';
  const s = d.replace('T', ' ');
  const [datePart, timePart] = s.split(' ');
  const parts = datePart.split('-');
  if (parts.length === 3 && parts[0].length === 4) {
    const time = timePart ? ' ' + timePart.substring(0, 5) : '';
    return `${parts[2]}/${parts[1]}/${parts[0]}${time}`;
  }
  return d;
}

function fmtCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  const isNeg = value < 0;
  return `${isNeg ? '-' : ''}£${Math.abs(value).toLocaleString('en-GB', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function asBool(val: boolean | number | null | undefined): boolean {
  return val === true || val === 1;
}

const statusColors: Record<string, string> = {
  received: 'bg-blue-100 text-blue-800',
  processing: 'bg-yellow-100 text-yellow-800',
  reconciled: 'bg-green-100 text-green-800',
  acknowledged: 'bg-indigo-100 text-indigo-800',
  approved: 'bg-emerald-100 text-emerald-800',
  sent: 'bg-gray-100 text-gray-800',
  error: 'bg-red-100 text-red-800',
};

// ---------------------------------------------------------------------------
// Toggle Switch sub-component
// ---------------------------------------------------------------------------

interface ToggleSwitchProps {
  flag: 'reconciliation_active' | 'auto_respond' | 'never_communicate';
  value: boolean | number;
  label: string;
  description: string;
  activeColor?: string;
  busy: boolean;
  onToggle: (flag: 'reconciliation_active' | 'auto_respond' | 'never_communicate') => void;
}

function ToggleSwitch({ flag, value, label, description, activeColor = 'bg-blue-600', busy, onToggle }: ToggleSwitchProps) {
  const on = asBool(value);
  return (
    <div className="flex items-start justify-between py-3 border-b border-gray-100 last:border-0">
      <div>
        <p className="text-sm font-medium text-gray-900">{label}</p>
        <p className="text-xs text-gray-500 mt-0.5">{description}</p>
      </div>
      <button
        onClick={() => onToggle(flag)}
        disabled={busy}
        title={on ? `Disable ${label}` : `Enable ${label}`}
        className={`relative inline-flex h-6 w-11 flex-shrink-0 items-center rounded-full transition-colors disabled:opacity-50 mt-0.5 ${
          on ? activeColor : 'bg-gray-200'
        }`}
      >
        <span
          className={`inline-block h-4 w-4 transform rounded-full bg-white shadow transition-transform ${
            on ? 'translate-x-6' : 'translate-x-1'
          }`}
        />
      </button>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function SupplierAccountDetail() {
  const { account } = useParams<{ account: string }>();
  const navigate = useNavigate();

  const [detail, setDetail] = useState<SupplierDetailResponse | null>(null);
  const [comms, setComms] = useState<Communication[]>([]);
  const [loading, setLoading] = useState(true);
  const [commsLoading, setCommsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [savingFlag, setSavingFlag] = useState<string | null>(null);
  const [flagError, setFlagError] = useState<string | null>(null);
  const [flagSuccess, setFlagSuccess] = useState<string | null>(null);

  useEffect(() => {
    if (!account) return;
    loadDetail(account);
    loadCommunications(account);
  }, [account]);

  const loadDetail = async (code: string) => {
    setLoading(true);
    setError(null);
    try {
      const res = await authFetch(`/api/supplier-config/${code}/detail`);
      const json: SupplierDetailResponse = await res.json();
      if (!json.success) throw new Error(json.error || 'Failed to load supplier detail');
      setDetail(json);
    } catch (e: any) {
      setError(e.message || 'Failed to load supplier detail');
    } finally {
      setLoading(false);
    }
  };

  const loadCommunications = async (code: string) => {
    setCommsLoading(true);
    try {
      const res = await authFetch(`/api/supplier-communications/${code}`);
      const json: CommunicationsResponse = await res.json();
      if (json.success) setComms(json.communications || []);
    } catch {
      // Non-fatal — comms are optional display
    } finally {
      setCommsLoading(false);
    }
  };

  const handleToggleFlag = async (flag: 'reconciliation_active' | 'auto_respond' | 'never_communicate') => {
    if (!account || !detail?.config) return;
    const current = detail.config[flag];
    const newValue = asBool(current) ? 0 : 1;
    setSavingFlag(flag);
    setFlagError(null);
    setFlagSuccess(null);
    try {
      const res = await authFetch(`/api/supplier-config/${account}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [flag]: newValue }),
      });
      const json = await res.json();
      if (!res.ok || !json.success) throw new Error(json.error || 'Update failed');
      setDetail(prev => {
        if (!prev?.config) return prev;
        return { ...prev, config: { ...prev.config, [flag]: newValue } };
      });
      setFlagSuccess('Settings saved');
      setTimeout(() => setFlagSuccess(null), 2000);
    } catch (e: any) {
      setFlagError(e.message || 'Failed to save setting');
    } finally {
      setSavingFlag(null);
    }
  };

  // --- Loading state ---
  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin h-8 w-8 border-2 border-blue-500 border-t-transparent rounded-full" />
      </div>
    );
  }

  if (error || !detail) {
    return (
      <div className="p-6">
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2 mb-4">
          <AlertTriangle className="w-4 h-4 text-red-500 mt-0.5" />
          <p className="text-sm text-red-700">{error || 'Supplier not found'}</p>
        </div>
        <button onClick={() => navigate(-1)} className="text-blue-600 hover:underline text-sm">Go back</button>
      </div>
    );
  }

  const config = detail.config;
  const supplierName = config?.name || detail.account;
  const balance = detail.balance ?? config?.balance ?? null;

  return (
    <div className="max-w-5xl mx-auto space-y-6 p-4">

      {/* ------------------------------------------------------------------ */}
      {/* Header                                                               */}
      {/* ------------------------------------------------------------------ */}
      <div className="flex items-center gap-3">
        <button
          onClick={() => navigate('/supplier/directory')}
          className="p-1.5 rounded-lg hover:bg-gray-100 transition-colors"
        >
          <ArrowLeft className="w-5 h-5 text-gray-600" />
        </button>
        <Building className="w-6 h-6 text-blue-600 flex-shrink-0" />
        <div className="flex-1 min-w-0">
          <h1 className="text-xl font-bold text-gray-900 truncate">{supplierName}</h1>
          <p className="text-sm text-gray-500">{detail.account}</p>
        </div>
        <div className="text-right flex-shrink-0">
          <p className="text-xs text-gray-400 uppercase tracking-wide">Balance</p>
          <p className={`text-lg font-bold ${
            balance == null ? 'text-gray-400' :
            balance > 0 ? 'text-red-600' : 'text-emerald-600'
          }`}>
            {fmtCurrency(balance)}
          </p>
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Unallocated Payments Warning                                          */}
      {/* ------------------------------------------------------------------ */}
      {detail.unallocated_payments && detail.unallocated_payments.length > 0 && (
        <div className="p-4 bg-amber-50 border border-amber-200 rounded-lg">
          <div className="flex items-start gap-3">
            <AlertTriangle className="w-5 h-5 text-amber-600 flex-shrink-0 mt-0.5" />
            <div className="flex-1">
              <p className="text-sm font-medium text-amber-800">
                {detail.unallocated_payments.length} unallocated payment{detail.unallocated_payments.length !== 1 ? 's' : ''} on this account ({fmtCurrency(detail.unallocated_total)})
              </p>
              <p className="text-xs text-amber-700 mt-1">These payments have not been allocated to invoices. This may affect statement reconciliation and supplier balances.</p>
              <div className="mt-2 space-y-1">
                {detail.unallocated_payments.map((p: any, i: number) => (
                  <div key={i} className="flex items-center justify-between text-xs text-amber-800 bg-amber-100 rounded px-2 py-1">
                    <span>{p.type}: {p.reference} — {p.date}</span>
                    <span className="font-medium">{fmtCurrency(p.balance)}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ------------------------------------------------------------------ */}
      {/* Automation Flags                                                      */}
      {/* ------------------------------------------------------------------ */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 border-b border-gray-200 flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-700">Automation Settings</h2>
          {flagSuccess && (
            <span className="flex items-center gap-1 text-xs text-emerald-700">
              <CheckCircle className="w-3.5 h-3.5" /> {flagSuccess}
            </span>
          )}
        </div>
        <div className="px-4 py-1">
          {flagError && (
            <div className="my-2 p-2 bg-red-50 border border-red-200 rounded text-xs text-red-700 flex items-center gap-1.5">
              <AlertTriangle className="w-3.5 h-3.5 flex-shrink-0" />
              {flagError}
              <button onClick={() => setFlagError(null)} className="ml-auto text-red-400 hover:text-red-600">×</button>
            </div>
          )}
          {config ? (
            <>
              <ToggleSwitch
                flag="reconciliation_active"
                value={config.reconciliation_active}
                label="Reconciliation Active"
                description="Include this supplier in the automated statement reconciliation process"
                activeColor="bg-blue-600"
                busy={savingFlag === 'reconciliation_active'}
                onToggle={handleToggleFlag}
              />
              <ToggleSwitch
                flag="auto_respond"
                value={config.auto_respond}
                label="Auto-respond"
                description="Automatically send reconciliation responses to this supplier"
                activeColor="bg-emerald-600"
                busy={savingFlag === 'auto_respond'}
                onToggle={handleToggleFlag}
              />
              <ToggleSwitch
                flag="never_communicate"
                value={config.never_communicate}
                label="Never Communicate"
                description="Suppress all automated outbound communications to this supplier"
                activeColor="bg-amber-500"
                busy={savingFlag === 'never_communicate'}
                onToggle={handleToggleFlag}
              />
            </>
          ) : (
            <p className="py-4 text-sm text-gray-400">No automation config found — sync from Opera to create it.</p>
          )}
        </div>
        {config?.last_synced && (
          <div className="px-4 py-2 bg-gray-50 border-t border-gray-100">
            <p className="text-xs text-gray-400">Last synced from Opera: {fmtDate(config.last_synced)}</p>
          </div>
        )}
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Opera Contact                                                         */}
      {/* ------------------------------------------------------------------ */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 border-b border-gray-200">
          <h2 className="text-sm font-semibold text-gray-700">Opera Contact</h2>
          <p className="text-xs text-gray-400 mt-0.5">Read from Opera zcontacts — edit in Opera to change</p>
        </div>
        <div className="px-4 py-4">
          {detail.opera_contact ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
              {detail.opera_contact.name && (
                <div className="flex items-start gap-2">
                  <User className="w-4 h-4 text-gray-400 mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="text-xs text-gray-400">Name</p>
                    <p className="text-sm text-gray-900">{detail.opera_contact.name}</p>
                  </div>
                </div>
              )}
              {detail.opera_contact.role && (
                <div className="flex items-start gap-2">
                  <Building className="w-4 h-4 text-gray-400 mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="text-xs text-gray-400">Position</p>
                    <p className="text-sm text-gray-900">{detail.opera_contact.role}</p>
                  </div>
                </div>
              )}
              {detail.opera_contact.email && (
                <div className="flex items-start gap-2">
                  <Mail className="w-4 h-4 text-gray-400 mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="text-xs text-gray-400">Email</p>
                    <a
                      href={`mailto:${detail.opera_contact.email}`}
                      className="text-sm text-blue-600 hover:underline"
                    >
                      {detail.opera_contact.email}
                    </a>
                  </div>
                </div>
              )}
              {(detail.opera_contact.phone || detail.opera_contact.mobile) && (
                <div className="flex items-start gap-2">
                  <Phone className="w-4 h-4 text-gray-400 mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="text-xs text-gray-400">Phone</p>
                    <p className="text-sm text-gray-900">
                      {detail.opera_contact.phone || detail.opera_contact.mobile}
                    </p>
                  </div>
                </div>
              )}
            </div>
          ) : (
            <p className="text-sm text-gray-400">No contact record found in Opera for this supplier.</p>
          )}
        </div>
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Statement History                                                     */}
      {/* ------------------------------------------------------------------ */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 border-b border-gray-200">
          <h2 className="text-sm font-semibold text-gray-700">Statement History</h2>
          <p className="text-xs text-gray-400 mt-0.5">{detail.statement_history.length} statement{detail.statement_history.length !== 1 ? 's' : ''} received</p>
        </div>
        {detail.statement_history.length === 0 ? (
          <div className="px-4 py-8 text-center">
            <FileText className="w-8 h-8 text-gray-300 mx-auto mb-2" />
            <p className="text-sm text-gray-400">No statements received yet</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 text-gray-600 text-xs">
                  <th className="px-4 py-2 text-left">Statement Date</th>
                  <th className="px-4 py-2 text-left">Received</th>
                  <th className="px-4 py-2 text-center">Status</th>
                  <th className="px-4 py-2 text-right">Lines</th>
                  <th className="px-4 py-2 text-right">Queries</th>
                  <th className="px-4 py-2 text-right">Closing Balance</th>
                </tr>
              </thead>
              <tbody>
                {detail.statement_history.map((stmt, i) => (
                  <tr
                    key={stmt.id}
                    onClick={() => navigate(`/supplier/statements/${stmt.id}`)}
                    className={`cursor-pointer hover:bg-blue-50 transition-colors ${i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}`}
                  >
                    <td className="px-4 py-2 text-gray-900 font-medium">{fmtDate(stmt.statement_date) || '—'}</td>
                    <td className="px-4 py-2 text-gray-500">{fmtDate(stmt.received_date) || '—'}</td>
                    <td className="px-4 py-2 text-center">
                      <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${statusColors[stmt.status] || 'bg-gray-100 text-gray-600'}`}>
                        {stmt.status}
                      </span>
                    </td>
                    <td className="px-4 py-2 text-right text-gray-600">{stmt.line_count ?? '—'}</td>
                    <td className="px-4 py-2 text-right">
                      {(stmt.query_count ?? 0) > 0 ? (
                        <span className="text-amber-700 font-medium">{stmt.query_count}</span>
                      ) : (
                        <span className="text-gray-400">0</span>
                      )}
                    </td>
                    <td className="px-4 py-2 text-right text-gray-700">{fmtCurrency(stmt.closing_balance)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* ------------------------------------------------------------------ */}
      {/* Audit Trail / Communications Log                                      */}
      {/* ------------------------------------------------------------------ */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 border-b border-gray-200">
          <h2 className="text-sm font-semibold text-gray-700">Communications Log</h2>
          <p className="text-xs text-gray-400 mt-0.5">All inbound and outbound emails, system events</p>
        </div>

        {commsLoading ? (
          <div className="px-4 py-8 flex items-center justify-center">
            <div className="animate-spin h-5 w-5 border-2 border-blue-500 border-t-transparent rounded-full" />
          </div>
        ) : comms.length === 0 ? (
          <div className="px-4 py-8 text-center">
            <Activity className="w-8 h-8 text-gray-300 mx-auto mb-2" />
            <p className="text-sm text-gray-400">No communications recorded</p>
          </div>
        ) : (
          <div className="divide-y divide-gray-100">
            {comms.map((comm) => {
              const isInbound = comm.direction === 'inbound';
              const isSystem = comm.comm_type === 'system_event';
              return (
                <div key={comm.id} className="px-4 py-3 flex items-start gap-3">
                  <div className={`mt-0.5 flex-shrink-0 rounded-full p-1.5 ${
                    isSystem
                      ? 'bg-gray-100 text-gray-500'
                      : isInbound
                      ? 'bg-blue-50 text-blue-600'
                      : 'bg-emerald-50 text-emerald-600'
                  }`}>
                    {isSystem ? (
                      <Clock className="w-3.5 h-3.5" />
                    ) : isInbound ? (
                      <ArrowDownLeft className="w-3.5 h-3.5" />
                    ) : (
                      <ArrowUpRight className="w-3.5 h-3.5" />
                    )}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
                        isSystem
                          ? 'bg-gray-100 text-gray-600'
                          : isInbound
                          ? 'bg-blue-100 text-blue-700'
                          : 'bg-emerald-100 text-emerald-700'
                      }`}>
                        {isSystem ? 'System' : isInbound ? 'Inbound' : 'Outbound'}
                      </span>
                      {comm.comm_type && comm.comm_type !== 'system_event' && (
                        <span className="text-xs text-gray-400">{comm.comm_type.replace(/_/g, ' ')}</span>
                      )}
                      <span className="text-xs text-gray-400 ml-auto flex-shrink-0">{fmtDateTime(comm.created_at)}</span>
                    </div>
                    {comm.email_subject && (
                      <p className="text-sm text-gray-900 mt-0.5 truncate">{comm.email_subject}</p>
                    )}
                    {comm.summary && (
                      <p className="text-xs text-gray-500 mt-0.5 line-clamp-2">{comm.summary}</p>
                    )}
                    {(comm.email_from || comm.email_to) && (
                      <p className="text-xs text-gray-400 mt-0.5">
                        {isInbound ? `From: ${comm.email_from}` : `To: ${comm.email_to}`}
                      </p>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

    </div>
  );
}
