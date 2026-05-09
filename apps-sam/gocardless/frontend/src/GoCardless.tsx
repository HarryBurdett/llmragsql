/**
 * GoCardless plugin entry component.
 *
 * Three-tab layout matching the legacy `GoCardlessImport.tsx`
 * structure:
 *   - Scan / Import   (scan-emails → preview → import)
 *   - History         (import-history list)
 *   - Settings        (company_reference, fees nominal, etc.)
 *
 * This scaffold renders the tab shell and exercises the new SAM
 * endpoints via `context.api.fetch`. The full UI port (~2,500 LOC of
 * matching tables, dropdowns, drag-handlers, dispute flows) is the
 * SAM team's job and lives at:
 *   frontend/src/pages/GoCardlessImport.tsx (legacy reference)
 *
 * Endpoints used here:
 *   - GET  /api/gocardless/health-check
 *   - GET  /api/gocardless/settings
 *   - GET  /api/gocardless/scan-emails
 *   - GET  /api/gocardless/import-history
 *
 * Endpoints the SAM team needs to wire next (already ported in
 * src/router.ts):
 *   - POST /api/gocardless/import
 *   - POST /api/gocardless/parse-content
 *   - POST /api/gocardless/skip-payout
 *   - POST /api/gocardless/archive-email
 *   - GET  /api/gocardless/payment-stats
 *   - ... see router.ts for the full list (40+ endpoints)
 */
import { useEffect, useState } from 'react';
import type { SamPluginContext } from './sam';

interface ScanResultBatch {
  email_id: number | null;
  email_subject: string | null;
  possible_duplicate: boolean;
  is_foreign_currency: boolean;
  batch: {
    gross_amount: number;
    net_amount: number;
    bank_reference: string | null;
    payment_count: number;
  };
}

interface ScanResponse {
  success: boolean;
  total_emails?: number;
  parsed_count?: number;
  batches?: ScanResultBatch[];
  error?: string;
}

interface SettingsResponse {
  success: boolean;
  settings?: Record<string, unknown>;
  error?: string;
}

interface HealthResponse {
  success: boolean;
  errors?: string[];
  warnings?: string[];
}

type Tab = 'scan' | 'history' | 'settings';

export default function GoCardless({
  context,
}: {
  context: SamPluginContext;
}) {
  const [tab, setTab] = useState<Tab>('scan');
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [scan, setScan] = useState<ScanResponse | null>(null);
  const [settings, setSettings] = useState<SettingsResponse | null>(null);
  const [scanning, setScanning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    context.api
      .fetch<HealthResponse>('/api/gocardless/health-check')
      .then((r) => {
        if (!cancelled) setHealth(r);
      })
      .catch((e) => {
        if (!cancelled) setError(e?.message ?? String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [context.api]);

  useEffect(() => {
    if (tab !== 'settings') return;
    context.api
      .fetch<SettingsResponse>('/api/gocardless/settings')
      .then(setSettings)
      .catch((e) => setError(e?.message ?? String(e)));
  }, [tab, context.api]);

  const runScan = async () => {
    setScanning(true);
    setError(null);
    try {
      const result = await context.api.fetch<ScanResponse>(
        '/api/gocardless/scan-emails',
      );
      setScan(result);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setError(msg);
    } finally {
      setScanning(false);
    }
  };

  return (
    <div className="gocardless-app space-y-4">
      <header className="flex items-baseline justify-between">
        <div>
          <h1 className="text-2xl font-bold">GoCardless</h1>
          <p className="text-sm text-gray-500">
            Direct Debit payout import for{' '}
            {context.currentCompany?.name ?? '—'}
          </p>
        </div>
        {health && (
          <span
            className={
              health.success
                ? 'rounded-full bg-green-100 px-3 py-1 text-xs text-green-700'
                : 'rounded-full bg-red-100 px-3 py-1 text-xs text-red-700'
            }
          >
            {health.success ? 'Connected' : 'Health check failed'}
          </span>
        )}
      </header>

      <nav className="flex gap-2 border-b border-gray-200">
        {(['scan', 'history', 'settings'] as const).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={
              tab === t
                ? 'border-b-2 border-blue-600 px-4 py-2 text-sm font-medium text-blue-600'
                : 'px-4 py-2 text-sm text-gray-500 hover:text-gray-700'
            }
          >
            {t === 'scan'
              ? 'Scan & Import'
              : t === 'history'
              ? 'History'
              : 'Settings'}
          </button>
        ))}
      </nav>

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

      {tab === 'scan' && (
        <ScanTab
          scan={scan}
          scanning={scanning}
          onScan={runScan}
        />
      )}
      {tab === 'history' && <HistoryTab context={context} />}
      {tab === 'settings' && <SettingsTab settings={settings} />}
    </div>
  );
}

function ScanTab({
  scan,
  scanning,
  onScan,
}: {
  scan: ScanResponse | null;
  scanning: boolean;
  onScan: () => void;
}) {
  return (
    <div className="space-y-4">
      <button
        onClick={onScan}
        disabled={scanning}
        className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:bg-gray-300"
      >
        {scanning ? 'Scanning…' : 'Scan inbox for GoCardless emails'}
      </button>

      {scan?.batches && scan.batches.length === 0 && (
        <p className="text-sm text-gray-500">No new payout emails found.</p>
      )}

      {scan?.batches && scan.batches.length > 0 && (
        <table className="w-full text-sm">
          <thead className="text-left text-xs uppercase text-gray-500">
            <tr>
              <th className="py-2">Reference</th>
              <th>Payments</th>
              <th>Gross</th>
              <th>Net</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {scan.batches.map((b, idx) => (
              <tr key={`${b.email_id}-${idx}`} className="border-t">
                <td className="py-2 font-mono text-xs">
                  {b.batch.bank_reference ?? '—'}
                </td>
                <td>{b.batch.payment_count}</td>
                <td>£{b.batch.gross_amount.toFixed(2)}</td>
                <td>£{b.batch.net_amount.toFixed(2)}</td>
                <td>
                  {b.possible_duplicate && (
                    <span className="rounded bg-amber-100 px-2 py-0.5 text-xs text-amber-700">
                      Possible duplicate
                    </span>
                  )}
                  {b.is_foreign_currency && (
                    <span className="ml-1 rounded bg-amber-100 px-2 py-0.5 text-xs text-amber-700">
                      Foreign currency
                    </span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

interface HistoryRecord {
  id: number;
  bank_reference: string | null;
  gross_amount: number;
  net_amount: number;
  payment_count: number;
  imported_at: string;
}

function HistoryTab({ context }: { context: SamPluginContext }) {
  const [records, setRecords] = useState<HistoryRecord[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    context.api
      .fetch<{ success: boolean; records?: HistoryRecord[]; error?: string }>(
        '/api/gocardless/import-history',
      )
      .then((r) => {
        if (r.success) setRecords(r.records ?? []);
        else setError(r.error ?? 'Failed to load history');
      })
      .catch((e: unknown) => {
        const msg = e instanceof Error ? e.message : String(e);
        setError(msg);
      });
  }, [context.api]);

  if (error) return <p className="text-sm text-red-600">{error}</p>;
  if (!records) return <p className="text-sm text-gray-500">Loading…</p>;
  if (records.length === 0)
    return <p className="text-sm text-gray-500">No imports yet.</p>;

  return (
    <table className="w-full text-sm">
      <thead className="text-left text-xs uppercase text-gray-500">
        <tr>
          <th className="py-2">Reference</th>
          <th>Imported</th>
          <th>Payments</th>
          <th>Gross</th>
          <th>Net</th>
        </tr>
      </thead>
      <tbody>
        {records.map((r) => (
          <tr key={r.id} className="border-t">
            <td className="py-2 font-mono text-xs">
              {r.bank_reference ?? '—'}
            </td>
            <td>{new Date(r.imported_at).toLocaleString()}</td>
            <td>{r.payment_count}</td>
            <td>£{r.gross_amount.toFixed(2)}</td>
            <td>£{r.net_amount.toFixed(2)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function SettingsTab({
  settings,
}: {
  settings: SettingsResponse | null;
}) {
  if (!settings) return <p className="text-sm text-gray-500">Loading…</p>;
  if (!settings.success)
    return <p className="text-sm text-red-600">{settings.error}</p>;
  return (
    <pre className="overflow-auto rounded-lg bg-gray-50 p-4 text-xs text-gray-700">
      {JSON.stringify(settings.settings, null, 2)}
    </pre>
  );
}
