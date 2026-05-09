/**
 * Bank Reconciliation plugin entry component.
 *
 * Five-stage workflow shell matching the legacy
 * `BankStatementReconcile.tsx` (5,436 LOC at
 * frontend/src/pages/BankStatementReconcile.tsx — the SAM team's
 * canonical reference for the full UI port):
 *   1. Select bank + statement source
 *   2. Review & match (AI extraction → cashbook matches)
 *   3. Import to Opera (post unmatched + auto-allocate)
 *   4. Reconcile (mirror statement order, running balance)
 *   5. Complete (close the statement)
 *
 * This scaffold renders the bank picker + the scan-emails table so
 * the operator can see the new mailbox-driven flow working end-to-end
 * against the new endpoints.
 *
 * Endpoints used:
 *   - GET  /api/reconcile/banks
 *   - GET  /api/bank-import/scan-emails?bank_code=...
 *   - GET  /api/bank-import/import-history
 *
 * Endpoints already ported in src/router.ts that the SAM team
 * needs to wire next:
 *   - POST /api/bank-import/import-from-pdf
 *   - POST /api/reconcile/bank/:bank_code/mark-reconciled
 *   - POST /api/bank-reconciliation/complete
 *   - ... see router.ts for the full list (43 endpoints)
 */
import { useEffect, useState } from 'react';
import type { SamPluginContext } from './sam';

interface BankRow {
  nk_acnt: string;
  nk_desc: string;
  nk_curbal?: number;
  nk_recbal?: number;
}

interface BanksResponse {
  success: boolean;
  banks?: BankRow[];
  error?: string;
}

interface ScanAttachment {
  attachment_id: string;
  filename: string;
  size_bytes?: number;
  statement_date?: string | null;
}

interface ScanCandidate {
  email_id: number;
  subject: string | null;
  detected_bank: string | null;
  statement_date: string | null;
  attachments: ScanAttachment[];
  validation_status: 'pending' | 'unsupported' | string;
}

interface ScanResponse {
  success: boolean;
  reconciled_balance?: number | null;
  total_emails_scanned?: number;
  total_pdfs_found?: number;
  already_processed_count?: number;
  statements?: ScanCandidate[];
  error?: string;
}

export default function BankReconcile({
  context,
}: {
  context: SamPluginContext;
}) {
  const [banks, setBanks] = useState<BankRow[] | null>(null);
  const [bankCode, setBankCode] = useState<string>('');
  const [scan, setScan] = useState<ScanResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    context.api
      .fetch<BanksResponse>('/api/reconcile/banks')
      .then((r) => {
        if (cancelled) return;
        if (!r.success) {
          setError(r.error ?? 'Failed to load banks');
          return;
        }
        setBanks(r.banks ?? []);
        if (r.banks && r.banks.length > 0 && !bankCode) {
          const first = r.banks[0];
          if (first) setBankCode(first.nk_acnt.trim());
        }
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : String(e));
        }
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [context.api]);

  const runScan = async () => {
    if (!bankCode) return;
    setLoading(true);
    setError(null);
    try {
      const result = await context.api.fetch<ScanResponse>(
        `/api/bank-import/scan-emails?bank_code=${encodeURIComponent(bankCode)}`,
      );
      setScan(result);
      if (!result.success) {
        setError(result.error ?? 'Scan failed');
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bank-reconcile-app space-y-4">
      <header>
        <h1 className="text-2xl font-bold">Bank Reconciliation</h1>
        <p className="text-sm text-gray-500">
          {context.currentCompany?.name ?? '—'}
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

      <section className="flex items-end gap-3">
        <label className="text-sm">
          <span className="block text-xs text-gray-500">Bank account</span>
          <select
            value={bankCode}
            onChange={(e) => setBankCode(e.target.value)}
            className="mt-1 rounded border border-gray-300 px-2 py-1"
          >
            {(banks ?? []).map((b) => (
              <option key={b.nk_acnt} value={b.nk_acnt.trim()}>
                {b.nk_acnt.trim()} — {b.nk_desc}
              </option>
            ))}
          </select>
        </label>
        <button
          onClick={runScan}
          disabled={!bankCode || loading}
          className="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:bg-gray-300"
        >
          {loading ? 'Scanning…' : 'Scan inbox for statements'}
        </button>
      </section>

      {scan?.statements && (
        <section>
          <p className="mb-2 text-xs text-gray-500">
            {scan.total_emails_scanned ?? 0} emails scanned ·{' '}
            {scan.total_pdfs_found ?? 0} statement PDFs found ·{' '}
            {scan.already_processed_count ?? 0} already reconciled
            {scan.reconciled_balance !== null &&
              scan.reconciled_balance !== undefined && (
                <>
                  {' '}
                  · Opera reconciled balance £
                  {Number(scan.reconciled_balance).toFixed(2)}
                </>
              )}
          </p>
          {scan.statements.length === 0 ? (
            <p className="text-sm text-gray-500">
              No new bank statements found.
            </p>
          ) : (
            <table className="w-full text-sm">
              <thead className="text-left text-xs uppercase text-gray-500">
                <tr>
                  <th className="py-2">Date</th>
                  <th>Bank</th>
                  <th>Filename</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {scan.statements.map((s) => (
                  <tr key={s.email_id} className="border-t">
                    <td className="py-2">{s.statement_date ?? '—'}</td>
                    <td>{s.detected_bank ?? '—'}</td>
                    <td>
                      {s.attachments.map((a) => a.filename).join(', ')}
                    </td>
                    <td>
                      <span className="rounded bg-amber-100 px-2 py-0.5 text-xs text-amber-700">
                        {s.validation_status}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      )}
    </div>
  );
}
