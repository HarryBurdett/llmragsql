import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  ArrowRight, CheckCircle, XCircle, AlertTriangle, Play, RefreshCw,
  ChevronDown, ChevronRight, FileText, Package
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, Alert } from '../components/ui';

const STATUS_LABELS: Record<string, string> = {
  Q: 'Quote', P: 'Proforma', O: 'Order', D: 'Delivery', U: 'Despatched', I: 'Invoice', C: 'Credit',
};

interface SOPDocument {
  id: number;
  doc: string;
  quote: string;
  proforma: string;
  order: string;
  delivery: string;
  invoice: string;
  account: string;
  name: string;
  cust_ref: string;
  date: string;
  due_date: string;
  ex_vat: number;
  vat: number;
  total: number;
  warehouse: string;
  route: string;
  status: string;
  priority: number;
  line_count: number;
}

interface Progression {
  from: string;
  from_label: string;
  to: string;
  to_label: string;
  available: boolean;
}

export default function SOPBatchProcessing() {
  const queryClient = useQueryClient();
  const [selectedProgression, setSelectedProgression] = useState<Progression | null>(null);
  const [selectedDocs, setSelectedDocs] = useState<Set<number>>(new Set());
  const [dueDateTo, setDueDateTo] = useState('');
  const [numberFrom, setNumberFrom] = useState('');
  const [numberTo, setNumberTo] = useState('');
  const [postingDate, setPostingDate] = useState(new Date().toISOString().split('T')[0]);
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);
  const [results, setResults] = useState<any[] | null>(null);
  const [expandedDoc, setExpandedDoc] = useState<number | null>(null);

  // Load SOP config
  const { data: configData } = useQuery({
    queryKey: ['sop-config'],
    queryFn: async () => { const r = await authFetch('/api/sop/config'); return r.json(); },
  });

  const progressions: Progression[] = configData?.progressions || [];
  const config = configData?.config || {};

  // Load documents for selected progression
  const { data: docsData, refetch: refetchDocs, isFetching } = useQuery({
    queryKey: ['sop-documents', selectedProgression?.from, dueDateTo, numberFrom, numberTo],
    queryFn: async () => {
      if (!selectedProgression) return null;
      const params = new URLSearchParams({ status: selectedProgression.from });
      if (dueDateTo) params.set('due_date_to', dueDateTo);
      if (numberFrom) params.set('number_from', numberFrom);
      if (numberTo) params.set('number_to', numberTo);
      const r = await authFetch(`/api/sop/documents?${params}`);
      return r.json();
    },
    enabled: !!selectedProgression,
  });

  // Load lines for expanded document
  const { data: linesData } = useQuery({
    queryKey: ['sop-lines', expandedDoc],
    queryFn: async () => {
      if (!expandedDoc) return null;
      const r = await authFetch(`/api/sop/document/${expandedDoc}/lines`);
      return r.json();
    },
    enabled: !!expandedDoc,
  });

  const documents: SOPDocument[] = docsData?.documents || [];

  const toggleDoc = (id: number) => {
    setSelectedDocs(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const selectAll = () => {
    if (selectedDocs.size === documents.length) {
      setSelectedDocs(new Set());
    } else {
      setSelectedDocs(new Set(documents.map(d => d.id)));
    }
  };

  const handleProgress = async () => {
    if (!selectedProgression || selectedDocs.size === 0) return;
    setProcessing(true);
    setError(null);
    setSuccess(null);
    setResults(null);

    try {
      const res = await authFetch('/api/sop/progress', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          doc_ids: Array.from(selectedDocs),
          from_status: selectedProgression.from,
          to_status: selectedProgression.to,
          posting_date: postingDate,
        }),
      });
      const data = await res.json();
      setResults(data.results || []);

      if (data.success) {
        setSuccess(`${data.succeeded} document(s) progressed to ${STATUS_LABELS[selectedProgression.to]}`);
        setSelectedDocs(new Set());
        refetchDocs();
        queryClient.invalidateQueries({ queryKey: ['sop-config'] });
      } else {
        if (data.failed > 0 && data.succeeded > 0) {
          setSuccess(`${data.succeeded} succeeded, ${data.failed} failed`);
        } else {
          setError(data.error || `${data.failed} document(s) failed`);
        }
      }
    } catch (e) {
      setError(`Failed: ${e}`);
    } finally {
      setProcessing(false);
    }
  };

  // Get the reference number column label based on status
  const getRefLabel = (status: string) => {
    switch (status) {
      case 'Q': return 'Quote';
      case 'P': return 'Proforma';
      case 'O': return 'Order';
      case 'D': case 'U': return 'Delivery';
      default: return 'Number';
    }
  };

  const getRefValue = (doc: SOPDocument) => {
    switch (doc.status) {
      case 'Q': return doc.quote;
      case 'P': return doc.proforma;
      case 'O': return doc.order;
      case 'D': case 'U': return doc.delivery;
      default: return doc.doc;
    }
  };

  return (
    <div className="space-y-6">
      <PageHeader title="SOP Batch Processing" subtitle="Progress sales documents through stages" icon={Package} />

      {error && <Alert variant="error" onDismiss={() => setError(null)}>{error}</Alert>}
      {success && <Alert variant="success" onDismiss={() => setSuccess(null)}>{success}</Alert>}

      {/* Config summary */}
      {config.sequences && (
        <Card>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-semibold text-gray-700">Configuration</h2>
          </div>
          <div className="flex flex-wrap gap-4 text-xs text-gray-600">
            <span>Delivery: {config.delivery_enabled ? 'Enabled' : 'Disabled'}</span>
            <span>Stock update at: {config.stock_update_at === 'D' ? 'Delivery' : config.stock_update_at === 'O' ? 'Order' : 'Invoice'}</span>
            <span className="text-gray-300">|</span>
            <span>Next Quote: {config.sequences.next_quote}</span>
            <span>Next Proforma: {config.sequences.next_proforma}</span>
            <span>Next Order: {config.sequences.next_order}</span>
            <span>Next Delivery: {config.sequences.next_delivery}</span>
            <span>Next Invoice: {config.sequences.next_invoice}</span>
          </div>
        </Card>
      )}

      {/* Progression selection + filters */}
      <Card>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-lg font-semibold text-gray-900">Select Progression</h2>
        </div>

        {/* Progression buttons */}
        <div className="flex flex-wrap gap-2 mb-4">
          {progressions.map(p => (
            <button
              key={`${p.from}_${p.to}`}
              onClick={() => {
                setSelectedProgression(p);
                setSelectedDocs(new Set());
                setResults(null);
              }}
              className={`px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-2 transition-colors ${
                selectedProgression?.from === p.from && selectedProgression?.to === p.to
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
              }`}
            >
              {p.from_label} <ArrowRight className="w-3 h-3" /> {p.to_label}
            </button>
          ))}
        </div>

        {/* Filters */}
        {selectedProgression && (
          <div className="grid grid-cols-4 gap-3 pt-3 border-t border-gray-100">
            <div>
              <label className="label">{getRefLabel(selectedProgression.from)} From</label>
              <input type="text" className="input" value={numberFrom} onChange={e => setNumberFrom(e.target.value)} placeholder="e.g. QUO00050" />
            </div>
            <div>
              <label className="label">{getRefLabel(selectedProgression.from)} To</label>
              <input type="text" className="input" value={numberTo} onChange={e => setNumberTo(e.target.value)} placeholder="e.g. QUO00107" />
            </div>
            <div>
              <label className="label">Due Date To</label>
              <input type="date" className="input" value={dueDateTo} onChange={e => setDueDateTo(e.target.value)} />
            </div>
            <div>
              <label className="label">Posting Date</label>
              <input type="date" className="input" value={postingDate} onChange={e => setPostingDate(e.target.value)} />
            </div>
          </div>
        )}
      </Card>

      {/* Document list */}
      {selectedProgression && (
        <Card>
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-lg font-semibold text-gray-900">
                {STATUS_LABELS[selectedProgression.from]}s to Progress
              </h2>
              <p className="text-sm text-gray-500">
                {documents.length} document(s) — select and click Progress
              </p>
            </div>
            <div className="flex gap-2">
              <button onClick={() => refetchDocs()} className="p-2 text-gray-400 hover:text-gray-600">
                <RefreshCw className={`w-4 h-4 ${isFetching ? 'animate-spin' : ''}`} />
              </button>
              {selectedDocs.size > 0 && (
                <button
                  onClick={handleProgress}
                  disabled={processing}
                  className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:bg-green-400 flex items-center gap-2 text-sm"
                >
                  <Play className="w-4 h-4" />
                  {processing ? 'Processing...' : `Progress ${selectedDocs.size} to ${STATUS_LABELS[selectedProgression.to]}`}
                </button>
              )}
            </div>
          </div>

          {documents.length === 0 ? (
            <p className="text-sm text-gray-500 py-4 text-center">No documents at {STATUS_LABELS[selectedProgression.from]} status matching filters</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-3 py-2 w-10">
                      <input type="checkbox" checked={selectedDocs.size === documents.length && documents.length > 0}
                        onChange={selectAll} className="rounded border-gray-300 text-green-600" />
                    </th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">{getRefLabel(selectedProgression.from)}</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Date</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Customer</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Cust Ref</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">W/H</th>
                    <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Route</th>
                    <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">Goods</th>
                    <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">Total</th>
                    <th className="px-3 py-2 text-center text-xs font-medium text-gray-500">Lines</th>
                    <th className="px-3 py-2"></th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-100">
                  {documents.map(doc => (
                    <>
                      <tr key={doc.id} className={selectedDocs.has(doc.id) ? 'bg-green-50' : ''}>
                        <td className="px-3 py-2">
                          <input type="checkbox" checked={selectedDocs.has(doc.id)}
                            onChange={() => toggleDoc(doc.id)} className="rounded border-gray-300 text-green-600" />
                        </td>
                        <td className="px-3 py-2 text-sm font-medium text-gray-900">{getRefValue(doc)}</td>
                        <td className="px-3 py-2 text-sm text-gray-500">{doc.date}</td>
                        <td className="px-3 py-2 text-sm text-gray-900">
                          <div>{doc.name}</div>
                          <div className="text-xs text-gray-400">{doc.account}</div>
                        </td>
                        <td className="px-3 py-2 text-sm text-gray-500">{doc.cust_ref}</td>
                        <td className="px-3 py-2 text-sm text-gray-500">{doc.warehouse}</td>
                        <td className="px-3 py-2 text-sm text-gray-500">{doc.route}</td>
                        <td className="px-3 py-2 text-sm text-right text-gray-900">
                          {doc.ex_vat.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        </td>
                        <td className="px-3 py-2 text-sm text-right font-medium text-gray-900">
                          {doc.total.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                        </td>
                        <td className="px-3 py-2 text-sm text-center text-gray-500">{doc.line_count}</td>
                        <td className="px-3 py-2">
                          <button onClick={() => setExpandedDoc(expandedDoc === doc.id ? null : doc.id)}
                            className="text-gray-400 hover:text-blue-600">
                            {expandedDoc === doc.id ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
                          </button>
                        </td>
                      </tr>
                      {expandedDoc === doc.id && linesData?.lines && (
                        <tr key={`${doc.id}-lines`}>
                          <td colSpan={11} className="px-6 py-3 bg-gray-50">
                            <table className="min-w-full text-xs">
                              <thead>
                                <tr className="text-gray-500">
                                  <th className="px-2 py-1 text-left">Line</th>
                                  <th className="px-2 py-1 text-left">Stock</th>
                                  <th className="px-2 py-1 text-left">Description</th>
                                  <th className="px-2 py-1 text-right">Qty</th>
                                  <th className="px-2 py-1 text-right">Price</th>
                                  <th className="px-2 py-1 text-right">Ex VAT</th>
                                  <th className="px-2 py-1 text-right">VAT</th>
                                  <th className="px-2 py-1 text-right">Total</th>
                                </tr>
                              </thead>
                              <tbody>
                                {linesData.lines.map((line: any) => (
                                  <tr key={line.line_no}>
                                    <td className="px-2 py-1">{line.line_no}</td>
                                    <td className="px-2 py-1 font-mono">{line.stock_code}</td>
                                    <td className="px-2 py-1">{line.description}</td>
                                    <td className="px-2 py-1 text-right">{line.quantity}</td>
                                    <td className="px-2 py-1 text-right">{line.price.toFixed(2)}</td>
                                    <td className="px-2 py-1 text-right">{line.ex_vat.toFixed(2)}</td>
                                    <td className="px-2 py-1 text-right">{line.vat.toFixed(2)}</td>
                                    <td className="px-2 py-1 text-right font-medium">{line.line_total.toFixed(2)}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </td>
                        </tr>
                      )}
                    </>
                  ))}
                </tbody>
              </table>

              {/* Summary row */}
              <div className="flex justify-end gap-6 p-3 bg-gray-50 border-t text-sm">
                <span className="text-gray-500">Selected: {selectedDocs.size}</span>
                <span className="text-gray-700">Goods: £{docsData?.summary?.total_ex_vat?.toLocaleString('en-GB', { minimumFractionDigits: 2 }) || '0.00'}</span>
                <span className="font-semibold text-gray-900">Total: £{docsData?.summary?.total?.toLocaleString('en-GB', { minimumFractionDigits: 2 }) || '0.00'}</span>
              </div>
            </div>
          )}
        </Card>
      )}

      {/* Results */}
      {results && results.length > 0 && (
        <Card>
          <h2 className="text-lg font-semibold text-gray-900 mb-3">Results</h2>
          <div className="space-y-2">
            {results.map((r: any, i: number) => (
              <div key={i} className={`flex items-center gap-3 p-2 rounded ${r.success ? 'bg-green-50' : 'bg-red-50'}`}>
                {r.success ? <CheckCircle className="w-4 h-4 text-green-600" /> : <XCircle className="w-4 h-4 text-red-600" />}
                <span className="text-sm">
                  {r.doc_number}: {r.success
                    ? `Progressed to ${STATUS_LABELS[r.to_status]} — assigned ${r.assigned_number}`
                    : r.error}
                </span>
                {r.tables_updated && (
                  <span className="text-xs text-gray-400 ml-auto">{r.tables_updated.join(', ')}</span>
                )}
              </div>
            ))}
          </div>
        </Card>
      )}
    </div>
  );
}
