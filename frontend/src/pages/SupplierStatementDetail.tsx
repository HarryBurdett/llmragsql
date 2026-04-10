import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, FileText, CheckCircle, AlertTriangle, Clock, Eye, RefreshCw } from 'lucide-react';
import { authFetch } from '../api/client';

interface Statement {
  id: number;
  supplier_code: string;
  supplier_name: string;
  statement_date: string;
  received_date: string;
  status: string;
  sender_email: string;
  opening_balance: number;
  closing_balance: number;
  currency: string;
  line_count: number;
  matched_count: number;
  query_count: number;
  acknowledged_at: string | null;
  processed_at: string | null;
  approved_by: string | null;
  approved_at: string | null;
  sent_at: string | null;
  error_message: string | null;
  opera_balance: number | null;
  balance_difference: number | null;
  unallocated_payments?: { reference: string; date: string; balance: number; type: string }[];
  unallocated_total?: number;
}

interface StatementLine {
  id: number;
  line_date: string;
  reference: string;
  description: string;
  debit: number | null;
  credit: number | null;
  doc_type: string;
  exists_in_opera: string | null;
  status: string | null;
  matched_ptran_id: string | null;
  query_type: string | null;
}

interface Summary {
  total_lines: number;
  total_debits: number;
  total_credits: number;
  agreed_count: number;
  query_count: number;
  exists_yes: number;
  exists_no: number;
  opera_only_count: number;
  opera_only_net: number;
  not_ours_net: number;
  not_ours_count: number;
  amount_diffs_net: number;
}

const statusColors: Record<string, string> = {
  received: 'bg-blue-100 text-blue-800',
  processing: 'bg-yellow-100 text-yellow-800',
  reconciled: 'bg-green-100 text-green-800',
  acknowledged: 'bg-indigo-100 text-indigo-800',
  approved: 'bg-emerald-100 text-emerald-800',
  sent: 'bg-gray-100 text-gray-800',
};

const existsColors: Record<string, string> = {
  Yes: 'text-green-700 bg-green-50',
  No: 'text-red-700 bg-red-50',
};

const lineStatusColors: Record<string, string> = {
  Agreed: 'text-green-700 bg-green-50',
  Query: 'text-amber-700 bg-amber-50',
  'Amount Difference': 'text-red-700 bg-red-50',
};

function fmtDate(d: string | null | undefined): string {
  if (!d) return '';
  // Handle "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS" or ISO formats
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

export default function SupplierStatementDetail() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const [statement, setStatement] = useState<Statement | null>(null);
  const [lines, setLines] = useState<StatementLine[]>([]);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [operaOnly, setOperaOnly] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [processing, setProcessing] = useState(false);
  const [showPdf, setShowPdf] = useState(false);
  const [pdfUrl, setPdfUrl] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    if (id) {
      loadStatement(parseInt(id));
    }
  }, [id]);

  const loadStatement = async (stmtId: number) => {
    setLoading(true);
    try {
      const [stmtResp, linesResp] = await Promise.all([
        authFetch(`/api/supplier-statements/${stmtId}`),
        authFetch(`/api/supplier-statements/${stmtId}/lines`),
      ]);
      const stmtData = await stmtResp.json();
      const linesData = await linesResp.json();
      if (stmtData.success) setStatement(stmtData.statement);
      else setError(stmtData.error || 'Statement not found');
      if (linesData.success) {
        setLines(linesData.lines);
        setSummary(linesData.summary);
        setOperaOnly(linesData.opera_only || []);
      }
    } catch (e: any) {
      setError(e.message || 'Failed to load statement');
    } finally {
      setLoading(false);
    }
  };

  const handleProcess = async () => {
    if (!id) return;
    setProcessing(true);
    try {
      const resp = await authFetch(`/api/supplier-statements/${id}/process`, { method: 'POST' });
      const res = await resp.json();
      if (res.success) {
        loadStatement(parseInt(id));
      } else {
        setError(res.error || 'Processing failed');
      }
    } catch (e: any) {
      setError(e.message);
    } finally {
      setProcessing(false);
    }
  };

  const loadPdf = async () => {
    if (pdfUrl) {
      // Already loaded — just toggle visibility
      setShowPdf(!showPdf);
      return;
    }
    try {
      const resp = await authFetch(`/api/supplier-statements/${id}/pdf`);
      if (resp.ok) {
        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        setPdfUrl(url);
        setShowPdf(true);
      } else {
        setError('PDF not available for this statement');
      }
    } catch (e: any) {
      setError(e.message || 'Failed to load PDF');
    }
  };

  const [approving, setApproving] = useState(false);

  // Email preview/edit state
  const [showEmailPreview, setShowEmailPreview] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [emailRecipient, setEmailRecipient] = useState('');
  const [emailSubject, setEmailSubject] = useState('');
  const [emailBody, setEmailBody] = useState('');

  const handleOpenEmailPreview = async () => {
    if (!id) return;
    setPreviewLoading(true);
    setError(null);
    try {
      const resp = await authFetch(`/api/supplier-statements/${id}/preview-response`, { method: 'POST' });
      const res = await resp.json();
      if (res.success) {
        setEmailRecipient(res.recipient || '');
        setEmailSubject(res.subject || '');
        setEmailBody(res.body || '');
        setShowEmailPreview(true);
      } else {
        setError(res.error || 'Failed to generate email preview');
      }
    } catch (e: any) {
      setError(e.message || 'Failed to generate email preview');
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleSendEmail = async () => {
    if (!id) return;
    setSuccess(null);
    setError(null);
    setApproving(true);
    try {
      const resp = await authFetch(`/api/supplier-statements/${id}/approve`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ subject: emailSubject, body: emailBody }),
      });
      const res = await resp.json();
      if (res.success) {
        setSuccess(res.message + (res.recipient ? ` — sent to ${res.recipient}` : ''));
        setShowEmailPreview(false);
        loadStatement(parseInt(id));
      } else {
        setError(res.detail || res.error || 'Approval failed');
      }
    } catch (e: any) {
      setError(e.message);
    } finally {
      setApproving(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="animate-spin h-8 w-8 border-2 border-blue-500 border-t-transparent rounded-full" />
      </div>
    );
  }

  if (!statement) {
    return (
      <div className="p-6">
        <p className="text-red-600">Statement not found</p>
        <button onClick={() => navigate(-1)} className="mt-2 text-blue-600 hover:underline">Go back</button>
      </div>
    );
  }

  return (
    <div className="p-4 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex items-center gap-3 mb-4">
        <button onClick={() => navigate(-1)} className="p-1.5 rounded-lg hover:bg-gray-100">
          <ArrowLeft className="w-5 h-5" />
        </button>
        <FileText className="w-6 h-6 text-blue-600" />
        <div className="flex-1">
          <h1 className="text-xl font-bold text-gray-900">
            {statement.supplier_name} — Statement {fmtDate(statement.statement_date)}
          </h1>
          <p className="text-sm text-gray-500">
            Account: {statement.supplier_code} · From: {statement.sender_email} · Received: {fmtDate(statement.received_date)}
          </p>
        </div>
        <span className={`px-3 py-1 rounded-full text-sm font-medium ${statusColors[statement.status] || 'bg-gray-100'}`}>
          {statement.status}
        </span>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
          <AlertTriangle className="w-4 h-4 text-red-500 mt-0.5" />
          <p className="text-sm text-red-700">{error}</p>
          <button onClick={() => setError(null)} className="ml-auto text-red-400 hover:text-red-600">×</button>
        </div>
      )}
      {success && (
        <div className="mb-4 p-3 bg-green-50 border border-green-200 rounded-lg flex items-start gap-2">
          <CheckCircle className="w-4 h-4 text-green-500 mt-0.5" />
          <p className="text-sm text-green-700">{success}</p>
          <button onClick={() => setSuccess(null)} className="ml-auto text-green-400 hover:text-green-600">×</button>
        </div>
      )}

      {/* Reconciliation */}
      <div className="bg-white rounded-lg border p-4 mb-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Reconciliation</h3>
        {(() => {
          const fmt = (v: number) => {
            const prefix = v < 0 ? '-' : '';
            return `${prefix}£${Math.abs(v).toLocaleString('en-GB', { minimumFractionDigits: 2 })}`;
          };

          const theirBal = statement.closing_balance || 0;
          const ourBal = statement.opera_balance ?? 0;
          const diff = theirBal - ourBal;

          // Pre-calculated from API — these ALWAYS sum to the difference
          const notOursNet = summary?.not_ours_net || 0;
          const notOursCount = summary?.not_ours_count || lines.filter(l => l.exists_in_opera === 'No').length;
          const oursNotNet = summary?.opera_only_net || 0;
          const oursNotCount = summary?.opera_only_count || 0;
          const amountDiffsNet = summary?.amount_diffs_net || 0;

          return (
            <table className="w-full text-sm max-w-lg">
              <tbody>
                <tr className="border-b">
                  <td className="py-2 text-gray-600">Balance per their statement</td>
                  <td className="py-2 text-right font-medium w-36">{fmt(theirBal)}</td>
                </tr>
                <tr className="border-b">
                  <td className="py-2 text-gray-600">Balance per our records (Opera)</td>
                  <td className="py-2 text-right font-medium">{fmt(ourBal)}</td>
                </tr>
                <tr className="border-b border-gray-400">
                  <td className="py-2 font-semibold">Difference</td>
                  <td className={`py-2 text-right font-bold ${Math.abs(diff) < 0.01 ? 'text-green-600' : 'text-red-600'}`}>
                    {fmt(diff)}
                  </td>
                </tr>

                {Math.abs(diff) >= 0.01 && (
                  <>
                    <tr><td colSpan={2} className="pt-3 pb-1 text-xs font-semibold text-gray-500 uppercase">Represented by</td></tr>
                    {Math.abs(notOursNet) >= 0.01 && (
                      <tr className="border-b">
                        <td className="py-1.5 text-gray-600 pl-3">On their statement, not on ours ({notOursCount} items)</td>
                        <td className="py-1.5 text-right font-medium text-amber-600">{fmt(notOursNet)}</td>
                      </tr>
                    )}
                    {Math.abs(oursNotNet) >= 0.01 && (
                      <tr className="border-b">
                        <td className="py-1.5 text-gray-600 pl-3">On our account, not on theirs ({oursNotCount} items)</td>
                        <td className="py-1.5 text-right font-medium text-amber-600">{fmt(oursNotNet)}</td>
                      </tr>
                    )}
                    {Math.abs(amountDiffsNet) >= 0.01 && (
                      <tr className="border-b">
                        <td className="py-1.5 text-gray-600 pl-3">Amount differences on agreed items</td>
                        <td className="py-1.5 text-right font-medium text-amber-600">{fmt(amountDiffsNet)}</td>
                      </tr>
                    )}
                    <tr className="border-t border-gray-400">
                      <td className="py-2 font-semibold">Total</td>
                      <td className="py-2 text-right font-bold">{fmt(diff)}</td>
                    </tr>
                  </>
                )}

                {Math.abs(diff) < 0.01 && (
                  <tr>
                    <td colSpan={2} className="py-3 text-center text-green-600 font-semibold">Balances agree</td>
                  </tr>
                )}
              </tbody>
            </table>
          );
        })()}
      </div>

      {/* Unallocated payments warning */}
      {statement.unallocated_payments && statement.unallocated_payments.length > 0 && (
        <div className="p-3 bg-amber-50 border border-amber-200 rounded-lg">
          <div className="flex items-start gap-2">
            <AlertTriangle className="w-4 h-4 text-amber-600 flex-shrink-0 mt-0.5" />
            <div className="flex-1">
              <p className="text-sm font-medium text-amber-800">
                {statement.unallocated_payments.length} unallocated payment{statement.unallocated_payments.length !== 1 ? 's' : ''} on this account (£{Math.abs(statement.unallocated_total || 0).toLocaleString('en-GB', { minimumFractionDigits: 2 })})
              </p>
              <p className="text-xs text-amber-700 mt-0.5">These may need allocating to invoices before the reconciliation is accurate.</p>
              <div className="mt-1.5 flex flex-wrap gap-2">
                {statement.unallocated_payments.map((p: any, i: number) => (
                  <span key={i} className="text-xs bg-amber-100 text-amber-800 px-2 py-0.5 rounded">
                    {p.reference} {p.date} £{Math.abs(p.balance).toFixed(2)}
                  </span>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Action buttons */}
      <div className="flex gap-2 mb-4">
        <button
          onClick={loadPdf}
          className="flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50"
        >
          <Eye className="w-4 h-4" />
          {showPdf ? 'Hide PDF' : 'View PDF'}
        </button>
        {statement.status === 'received' && (
          <button
            onClick={handleProcess}
            disabled={processing}
            className="flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            <Clock className="w-4 h-4" />
            {processing ? 'Processing...' : 'Process & Reconcile'}
          </button>
        )}
        {(statement.status === 'reconciled' || statement.status === 'acknowledged') && (
          <button
            onClick={() => {
              if (statement.unallocated_payments && statement.unallocated_payments.length > 0) {
                const total = Math.abs(statement.unallocated_total || 0).toFixed(2);
                const confirmed = window.confirm(
                  `There are ${statement.unallocated_payments.length} unallocated payment(s) (£${total}) on this account.\n\n` +
                  `This may affect the accuracy of the reconciliation. You may want to allocate these in Opera first.\n\n` +
                  `Do you want to continue sending the response?`
                );
                if (!confirmed) return;
              }
              handleOpenEmailPreview();
            }}
            disabled={previewLoading || approving}
            className="flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-white bg-green-600 rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-wait"
          >
            {previewLoading ? (
              <><div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" /> Preparing...</>
            ) : (
              <><CheckCircle className="w-4 h-4" /> Approve & Send Response</>
            )}
          </button>
        )}
        {/* Refresh button — always visible so user can reload after changes in Opera */}
        <button
          onClick={async () => {
            if (!id) return;
            setRefreshing(true);
            setSuccess(null);
            await loadStatement(parseInt(id));
            setRefreshing(false);
            setSuccess('Data refreshed from Opera');
          }}
          disabled={refreshing}
          className="flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50"
          title="Refresh — reload data from Opera (use after allocating payments)"
        >
          <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} /> {refreshing ? 'Refreshing...' : 'Refresh'}
        </button>
        {statement.status === 'approved' && (
          <div className="flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-green-700 bg-green-50 border border-green-200 rounded-lg">
            <CheckCircle className="w-4 h-4" />
            Response Sent
          </div>
        )}
      </div>

      {/* Email Preview / Edit Panel */}
      {showEmailPreview && (
        <div className="mb-4 bg-white rounded-lg border border-green-200 overflow-hidden">
          <div className="px-4 py-3 bg-green-50 border-b border-green-200 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-green-800">Review & Send Response Email</h3>
            <button
              onClick={() => setShowEmailPreview(false)}
              className="text-green-600 hover:text-green-800 text-lg leading-none"
            >
              ×
            </button>
          </div>
          <div className="p-4 space-y-3">
            {/* Recipient — read-only */}
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">To</label>
              <div className="px-3 py-2 bg-gray-50 border border-gray-200 rounded text-sm text-gray-700">
                {emailRecipient || <span className="text-gray-400 italic">No recipient email found</span>}
              </div>
            </div>

            {/* Subject — editable */}
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Subject</label>
              <input
                type="text"
                value={emailSubject}
                onChange={e => setEmailSubject(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded text-sm focus:outline-none focus:ring-1 focus:ring-green-400"
              />
            </div>

            {/* Body — editable textarea showing HTML source */}
            <div>
              <label className="block text-xs font-medium text-gray-500 mb-1">Body (HTML)</label>
              <textarea
                value={emailBody}
                onChange={e => setEmailBody(e.target.value)}
                rows={16}
                className="w-full px-3 py-2 border border-gray-300 rounded text-sm font-mono focus:outline-none focus:ring-1 focus:ring-green-400 resize-y"
              />
            </div>

            {/* Actions */}
            <div className="flex gap-2 pt-1">
              <button
                onClick={handleSendEmail}
                disabled={approving || !emailRecipient}
                className="flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-white bg-green-600 rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-wait"
              >
                {approving ? (
                  <><div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin" /> Sending...</>
                ) : (
                  <><CheckCircle className="w-4 h-4" /> Send</>
                )}
              </button>
              <button
                onClick={() => setShowEmailPreview(false)}
                disabled={approving}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {/* PDF Viewer */}
      {showPdf && (
        <div className="mb-4 bg-white rounded-lg border overflow-hidden">
          <div className="px-4 py-2 bg-gray-50 border-b flex items-center justify-between">
            <h3 className="text-sm font-semibold text-gray-700">Original Statement PDF</h3>
            <button onClick={() => setShowPdf(false)} className="text-gray-400 hover:text-gray-600 text-lg">×</button>
          </div>
          {pdfUrl ? (
            <iframe
              src={pdfUrl}
              className="w-full border-0"
              style={{ height: '600px' }}
              title="Statement PDF"
            />
          ) : (
            <div className="flex items-center justify-center h-32 text-gray-400">Loading PDF...</div>
          )}
        </div>
      )}

      {/* Transaction lines */}
      <div className="bg-white rounded-lg border overflow-hidden">
        <div className="px-4 py-3 bg-gray-50 border-b">
          <h2 className="text-sm font-semibold text-gray-700">Statement Lines</h2>
          {summary && (
            <p className="text-xs text-gray-500 mt-0.5">
              {summary.agreed_count || 0} agreed
              {(summary.query_count || 0) > 0 ? ` · ${summary.query_count} queries` : ''}
              {(summary.amount_diffs_net && Math.abs(summary.amount_diffs_net) >= 0.01)
                ? ` · ${lines.filter(l => l.status === 'Amount Difference').length} amount differences`
                : ''}
              {' · '}Debits: £{(summary.total_debits || 0).toLocaleString('en-GB', { minimumFractionDigits: 2 })}
              {' · '}Credits: £{(summary.total_credits || 0).toLocaleString('en-GB', { minimumFractionDigits: 2 })}
            </p>
          )}
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-gray-600 text-xs">
                <th className="px-3 py-2 text-left">Date</th>
                <th className="px-3 py-2 text-left">Reference</th>
                <th className="px-3 py-2 text-left">Type</th>
                <th className="px-3 py-2 text-right">Debit</th>
                <th className="px-3 py-2 text-right">Credit</th>
                <th className="px-3 py-2 text-center">Exists</th>
                <th className="px-3 py-2 text-center">Status</th>
              </tr>
            </thead>
            <tbody>
              {lines.map((line, i) => (
                <tr key={line.id} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}>
                  <td className="px-3 py-1.5 text-gray-700">{fmtDate(line.line_date)}</td>
                  <td className="px-3 py-1.5 font-medium text-gray-900">{line.reference || ''}</td>
                  <td className="px-3 py-1.5 text-gray-600">{line.doc_type || ''}</td>
                  <td className="px-3 py-1.5 text-right text-gray-700">
                    {line.debit ? `£${line.debit.toLocaleString('en-GB', { minimumFractionDigits: 2 })}` : ''}
                  </td>
                  <td className="px-3 py-1.5 text-right text-gray-700">
                    {line.credit ? `£${line.credit.toLocaleString('en-GB', { minimumFractionDigits: 2 })}` : ''}
                  </td>
                  <td className="px-3 py-1.5 text-center">
                    {line.exists_in_opera && (
                      <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${existsColors[line.exists_in_opera] || 'bg-gray-100 text-gray-600'}`}>
                        {line.exists_in_opera}
                      </span>
                    )}
                  </td>
                  <td className="px-3 py-1.5 text-center">
                    {line.status && (
                      <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${lineStatusColors[line.status] || 'bg-gray-100 text-gray-600'}`}>
                        {line.status}
                      </span>
                    )}
                  </td>
                </tr>
              ))}
              {lines.length === 0 && (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-gray-400">No line items extracted</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* On Our Account But Not On Statement */}
      {operaOnly.length > 0 && (
        <div className="mt-4 bg-white rounded-lg border overflow-hidden">
          <div className="px-4 py-3 bg-amber-50 border-b">
            <h2 className="text-sm font-semibold text-amber-800">On Our Account — Not On Their Statement</h2>
            <p className="text-xs text-amber-600 mt-0.5">{operaOnly.length} transaction(s) on our supplier account that do not appear on this statement</p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 text-gray-600 text-xs">
                  <th className="px-3 py-2 text-left">Date</th>
                  <th className="px-3 py-2 text-left">Reference</th>
                  <th className="px-3 py-2 text-left">Type</th>
                  <th className="px-3 py-2 text-right">Debit</th>
                  <th className="px-3 py-2 text-right">Credit</th>
                </tr>
              </thead>
              <tbody>
                {operaOnly.map((item: any, i: number) => {
                  const sv = item.signed_value ?? item.amount ?? 0;
                  const isDebit = sv > 0;
                  const absAmt = Math.abs(sv);
                  return (
                    <tr key={i} className={i % 2 === 0 ? 'bg-white' : 'bg-gray-50/50'}>
                      <td className="px-3 py-1.5 text-gray-700">{fmtDate(item.line_date)}</td>
                      <td className="px-3 py-1.5 font-medium text-gray-900">{item.reference || ''}</td>
                      <td className="px-3 py-1.5 text-gray-600">{item.doc_type || ''}</td>
                      <td className="px-3 py-1.5 text-right text-gray-700">
                        {isDebit ? `£${absAmt.toLocaleString('en-GB', { minimumFractionDigits: 2 })}` : ''}
                      </td>
                      <td className="px-3 py-1.5 text-right text-gray-700">
                        {!isDebit ? `£${absAmt.toLocaleString('en-GB', { minimumFractionDigits: 2 })}` : ''}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Timeline */}
      <div className="mt-4 bg-white rounded-lg border p-4">
        <h3 className="text-sm font-semibold text-gray-700 mb-2">Timeline</h3>
        <div className="space-y-1.5 text-xs text-gray-600">
          <p>Received: {fmtDateTime(statement.received_date) || '—'}</p>
          {statement.acknowledged_at && <p>Acknowledged: {fmtDateTime(statement.acknowledged_at)}</p>}
          {statement.processed_at && <p>Processed: {fmtDateTime(statement.processed_at)}</p>}
          {statement.approved_at && <p>Approved by {statement.approved_by}: {fmtDateTime(statement.approved_at)}</p>}
          {statement.sent_at && <p>Response sent: {fmtDateTime(statement.sent_at)}</p>}
          {statement.error_message && <p className="text-red-600">Error: {statement.error_message}</p>}
        </div>
      </div>
    </div>
  );
}
