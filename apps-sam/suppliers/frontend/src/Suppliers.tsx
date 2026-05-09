/**
 * Suppliers plugin entry component.
 *
 * Tabbed dashboard wrapping the new TS endpoints:
 *   - Dashboard  (counts of pending / processing / approved / queries)
 *   - Queue      (statements awaiting processing)
 *   - Queries    (open queries + overdue + auto-resolve)
 *   - History    (sent/approved statements)
 *
 * Legacy reference pages the SAM team should consult when porting
 * the full UI (this scaffold only renders the dashboard tile counts
 * and queue list):
 *   frontend/src/pages/SupplierDashboard.tsx
 *   frontend/src/pages/SupplierQueries.tsx
 *   frontend/src/pages/SupplierStatementHistory.tsx
 *   frontend/src/pages/SupplierAccount.tsx
 *   frontend/src/pages/SupplierSettings.tsx
 *   frontend/src/pages/SupplierReconciliations.tsx
 *
 * Endpoints used:
 *   - GET /api/supplier-statements/dashboard
 *   - GET /api/supplier-statements/queue
 *   - GET /api/supplier-queries
 */
import { useEffect, useState } from 'react';
import type { SamPluginContext } from './sam';

interface DashboardCounts {
  pending: number;
  processing: number;
  resolved: number;
  approved: number;
  total_open_queries: number;
  overdue_queries: number;
  total_disputes: number;
}

interface DashboardResponse {
  success: boolean;
  counts?: DashboardCounts;
  error?: string;
}

interface QueueItem {
  id: number;
  supplier_code: string;
  statement_date: string | null;
  received_date: string | null;
  status: string;
  closing_balance: number;
  currency: string;
  line_count: number;
  matched_count: number;
  query_count: number;
}

interface QueueResponse {
  success: boolean;
  statements?: QueueItem[];
  count?: number;
  error?: string;
}

type Tab = 'dashboard' | 'queue' | 'queries';

export default function Suppliers({
  context,
}: {
  context: SamPluginContext;
}) {
  const [tab, setTab] = useState<Tab>('dashboard');
  const [dash, setDash] = useState<DashboardResponse | null>(null);
  const [queue, setQueue] = useState<QueueResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    context.api
      .fetch<DashboardResponse>('/api/supplier-statements/dashboard')
      .then((r) => {
        if (!cancelled) setDash(r);
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [context.api]);

  useEffect(() => {
    if (tab !== 'queue') return;
    let cancelled = false;
    context.api
      .fetch<QueueResponse>('/api/supplier-statements/queue')
      .then((r) => {
        if (!cancelled) setQueue(r);
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [tab, context.api]);

  return (
    <div className="space-y-4">
      <header>
        <h1 className="text-2xl font-bold">Suppliers</h1>
        <p className="text-sm text-gray-500">
          Statement reconciliation for {context.currentCompany?.name ?? '—'}
        </p>
      </header>

      {error && (
        <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
          <button
            onClick={() => setError(null)}
            className="float-right text-red-400 hover:text-red-600"
          >
            ×
          </button>
        </div>
      )}

      <nav className="flex gap-2 border-b border-gray-200">
        {(['dashboard', 'queue', 'queries'] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={
              tab === t
                ? 'border-b-2 border-blue-600 px-4 py-2 text-sm font-medium text-blue-600'
                : 'px-4 py-2 text-sm text-gray-500 hover:text-gray-700'
            }
          >
            {t === 'dashboard'
              ? 'Dashboard'
              : t === 'queue'
              ? 'Queue'
              : 'Queries'}
          </button>
        ))}
      </nav>

      {tab === 'dashboard' && <Dashboard dash={dash} />}
      {tab === 'queue' && <Queue queue={queue} />}
      {tab === 'queries' && <Queries context={context} />}
    </div>
  );
}

function Dashboard({ dash }: { dash: DashboardResponse | null }) {
  if (!dash) return <p className="text-sm text-gray-500">Loading…</p>;
  if (!dash.success || !dash.counts)
    return (
      <p className="text-sm text-red-600">{dash.error ?? 'Failed to load'}</p>
    );
  const c = dash.counts;
  return (
    <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
      <Tile label="Pending" value={c.pending} tone="amber" />
      <Tile label="Processing" value={c.processing} tone="blue" />
      <Tile label="Approved" value={c.approved} tone="green" />
      <Tile
        label="Open queries"
        value={c.total_open_queries}
        tone={c.overdue_queries > 0 ? 'red' : 'gray'}
        sublabel={
          c.overdue_queries > 0
            ? `${c.overdue_queries} overdue`
            : undefined
        }
      />
    </div>
  );
}

function Tile({
  label,
  value,
  tone,
  sublabel,
}: {
  label: string;
  value: number;
  tone: 'gray' | 'blue' | 'green' | 'amber' | 'red';
  sublabel?: string;
}) {
  const toneClass = {
    gray: 'bg-gray-50 text-gray-700',
    blue: 'bg-blue-50 text-blue-700',
    green: 'bg-green-50 text-green-700',
    amber: 'bg-amber-50 text-amber-700',
    red: 'bg-red-50 text-red-700',
  }[tone];
  return (
    <div className={`rounded-lg p-4 ${toneClass}`}>
      <div className="text-xs uppercase tracking-wide opacity-70">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value}</div>
      {sublabel && (
        <div className="mt-1 text-xs opacity-70">{sublabel}</div>
      )}
    </div>
  );
}

function Queue({ queue }: { queue: QueueResponse | null }) {
  if (!queue) return <p className="text-sm text-gray-500">Loading…</p>;
  if (!queue.success)
    return <p className="text-sm text-red-600">{queue.error}</p>;
  if (!queue.statements || queue.statements.length === 0)
    return <p className="text-sm text-gray-500">Queue is empty.</p>;
  return (
    <table className="w-full text-sm">
      <thead className="text-left text-xs uppercase text-gray-500">
        <tr>
          <th className="py-2">Supplier</th>
          <th>Statement date</th>
          <th>Received</th>
          <th>Status</th>
          <th className="text-right">Closing</th>
          <th className="text-right">Lines</th>
          <th className="text-right">Queries</th>
        </tr>
      </thead>
      <tbody>
        {queue.statements.map((s) => (
          <tr key={s.id} className="border-t">
            <td className="py-2 font-mono text-xs">{s.supplier_code}</td>
            <td>{s.statement_date ?? '—'}</td>
            <td>
              {s.received_date
                ? new Date(s.received_date).toLocaleDateString()
                : '—'}
            </td>
            <td>{s.status}</td>
            <td className="text-right">
              {s.currency} {s.closing_balance.toFixed(2)}
            </td>
            <td className="text-right">
              {s.matched_count}/{s.line_count}
            </td>
            <td className="text-right">{s.query_count}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

interface QueryItem {
  id: number;
  supplier_code: string;
  reference: string;
  amount: number;
  query_type: string;
  status: 'open' | 'resolved' | 'cancelled';
  description: string;
  created_at: string;
}

interface QueriesResponse {
  success: boolean;
  queries?: QueryItem[];
  count?: number;
  error?: string;
}

function Queries({ context }: { context: SamPluginContext }) {
  const [data, setData] = useState<QueriesResponse | null>(null);

  useEffect(() => {
    let cancelled = false;
    context.api
      .fetch<QueriesResponse>('/api/supplier-queries?status=open')
      .then((r) => {
        if (!cancelled) setData(r);
      })
      .catch(() => {
        // swallowed — parent handles errors
      });
    return () => {
      cancelled = true;
    };
  }, [context.api]);

  if (!data) return <p className="text-sm text-gray-500">Loading…</p>;
  if (!data.success)
    return <p className="text-sm text-red-600">{data.error}</p>;
  if (!data.queries || data.queries.length === 0)
    return <p className="text-sm text-gray-500">No open queries.</p>;

  return (
    <table className="w-full text-sm">
      <thead className="text-left text-xs uppercase text-gray-500">
        <tr>
          <th className="py-2">Supplier</th>
          <th>Reference</th>
          <th>Amount</th>
          <th>Type</th>
          <th>Description</th>
          <th>Opened</th>
        </tr>
      </thead>
      <tbody>
        {data.queries.map((q) => (
          <tr key={q.id} className="border-t">
            <td className="py-2 font-mono text-xs">{q.supplier_code}</td>
            <td>{q.reference}</td>
            <td>£{q.amount.toFixed(2)}</td>
            <td>{q.query_type}</td>
            <td className="max-w-md truncate">{q.description}</td>
            <td>{new Date(q.created_at).toLocaleDateString()}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
