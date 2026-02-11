import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ShoppingCart, ChevronRight, X, Filter } from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

interface SOPDocument {
  document: string;
  account: string;
  customer_name: string;
  document_date: string | null;
  status: string;
  status_desc: string;
  sales_order: string;
  invoice: string;
  delivery: string;
  credit_note: string;
  customer_ref: string;
  net_value: number;
  vat_value: number;
  gross_value: number;
  warehouse: string;
  currency: string;
}

interface SOPLine {
  line_number: number;
  stock_ref: string;
  description: string;
  quantity: number;
  unit_price: number;
  line_value: number;
  discount_percent: number;
  vat_code: string;
  vat_rate: number;
  warehouse: string;
}

interface SOPDetail {
  header: SOPDocument & {
    address_1: string;
    address_2: string;
    address_3: string;
    address_4: string;
    overall_discount: number;
    order_date: string | null;
    delivery_date: string | null;
    invoice_date: string | null;
    narrative_1: string;
    narrative_2: string;
  };
  lines: SOPLine[];
}

async function fetchDocuments(params: { status?: string; account?: string; limit?: number; offset?: number }) {
  const queryParams = new URLSearchParams();
  if (params.status) queryParams.set('status', params.status);
  if (params.account) queryParams.set('account', params.account);
  queryParams.set('limit', String(params.limit || 50));
  queryParams.set('offset', String(params.offset || 0));

  const response = await fetch(`${API_BASE}/api/sop/documents?${queryParams}`);
  if (!response.ok) throw new Error('Failed to fetch documents');
  return response.json();
}

async function fetchDocumentDetail(docNumber: string): Promise<SOPDetail> {
  const response = await fetch(`${API_BASE}/api/sop/documents/${encodeURIComponent(docNumber)}`);
  if (!response.ok) throw new Error('Failed to fetch document detail');
  return response.json();
}

function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  return new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(value);
}

function getStatusColor(status: string): string {
  switch (status) {
    case 'Q': return 'bg-gray-100 text-gray-700';
    case 'P': return 'bg-yellow-100 text-yellow-700';
    case 'O': return 'bg-blue-100 text-blue-700';
    case 'D': return 'bg-purple-100 text-purple-700';
    case 'I': return 'bg-green-100 text-green-700';
    case 'C': return 'bg-red-100 text-red-700';
    default: return 'bg-gray-100 text-gray-700';
  }
}

export function SalesOrders() {
  const [selectedStatus, setSelectedStatus] = useState('');
  const [accountFilter, setAccountFilter] = useState('');
  const [selectedDoc, setSelectedDoc] = useState<string | null>(null);
  const [showFilters, setShowFilters] = useState(false);
  const [page, setPage] = useState(0);
  const pageSize = 50;

  const { data: documentsData, isLoading } = useQuery({
    queryKey: ['sop-documents', selectedStatus, accountFilter, page],
    queryFn: () => fetchDocuments({
      status: selectedStatus,
      account: accountFilter,
      limit: pageSize,
      offset: page * pageSize,
    }),
  });

  const { data: documentDetail, isLoading: detailLoading } = useQuery({
    queryKey: ['sop-document-detail', selectedDoc],
    queryFn: () => fetchDocumentDetail(selectedDoc!),
    enabled: !!selectedDoc,
  });

  const documents: SOPDocument[] = documentsData?.documents || [];
  const totalCount = documentsData?.count || 0;
  const totalPages = Math.ceil(totalCount / pageSize);

  const statuses = [
    { value: '', label: 'All' },
    { value: 'Q', label: 'Quotes' },
    { value: 'O', label: 'Orders' },
    { value: 'D', label: 'Deliveries' },
    { value: 'I', label: 'Invoices' },
    { value: 'C', label: 'Credits' },
  ];

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-start">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <ShoppingCart className="h-7 w-7 text-blue-600" />
            Sales Order Processing
          </h2>
          <p className="text-gray-600 mt-1">View sales documents - quotes, orders, deliveries, invoices</p>
        </div>
      </div>

      <div className="flex gap-6">
        {/* Left Panel - Document List */}
        <div className="flex-1 space-y-4">
          {/* Filters */}
          <div className="card">
            <div className="flex gap-4 items-center">
              <div className="flex gap-2">
                {statuses.map((s) => (
                  <button
                    key={s.value}
                    onClick={() => { setSelectedStatus(s.value); setPage(0); }}
                    className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                      selectedStatus === s.value
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                    }`}
                  >
                    {s.label}
                  </button>
                ))}
              </div>
              <button
                onClick={() => setShowFilters(!showFilters)}
                className={`px-3 py-1.5 border rounded-lg text-sm flex items-center gap-1 ${
                  showFilters || accountFilter ? 'bg-blue-50 border-blue-300' : 'border-gray-300'
                }`}
              >
                <Filter className="h-4 w-4" />
                More
              </button>
            </div>

            {showFilters && (
              <div className="mt-4 pt-4 border-t border-gray-200">
                <div className="flex gap-4 items-end">
                  <div className="flex-1">
                    <label className="block text-sm font-medium text-gray-700 mb-1">Customer Account</label>
                    <input
                      type="text"
                      value={accountFilter}
                      onChange={(e) => setAccountFilter(e.target.value)}
                      placeholder="e.g. ABC001"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                    />
                  </div>
                  <button
                    onClick={() => { setAccountFilter(''); setPage(0); }}
                    className="px-3 py-2 text-sm text-gray-600 hover:text-gray-900"
                  >
                    Clear
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Documents Table */}
          <div className="card">
            <div className="flex justify-between items-center mb-4">
              <h3 className="font-semibold">
                Documents {totalCount > 0 && <span className="text-gray-500 font-normal">({totalCount})</span>}
              </h3>
              {totalPages > 1 && (
                <div className="flex items-center gap-2 text-sm">
                  <button
                    onClick={() => setPage(Math.max(0, page - 1))}
                    disabled={page === 0}
                    className="px-2 py-1 border rounded disabled:opacity-50"
                  >
                    Previous
                  </button>
                  <span>Page {page + 1} of {totalPages}</span>
                  <button
                    onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
                    disabled={page >= totalPages - 1}
                    className="px-2 py-1 border rounded disabled:opacity-50"
                  >
                    Next
                  </button>
                </div>
              )}
            </div>

            {isLoading ? (
              <div className="text-center py-8 text-gray-500">Loading...</div>
            ) : documents.length === 0 ? (
              <div className="text-center py-8 text-gray-500">No documents found</div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-2 px-3 font-medium">Document</th>
                      <th className="text-left py-2 px-3 font-medium">Customer</th>
                      <th className="text-left py-2 px-3 font-medium">Date</th>
                      <th className="text-left py-2 px-3 font-medium">Status</th>
                      <th className="text-right py-2 px-3 font-medium">Net</th>
                      <th className="text-right py-2 px-3 font-medium">Gross</th>
                      <th className="w-8"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {documents.map((doc) => (
                      <tr
                        key={doc.document}
                        onClick={() => setSelectedDoc(doc.document)}
                        className={`border-b border-gray-100 cursor-pointer transition-colors ${
                          selectedDoc === doc.document ? 'bg-blue-50' : 'hover:bg-gray-50'
                        }`}
                      >
                        <td className="py-2 px-3 font-mono text-blue-600">{doc.document}</td>
                        <td className="py-2 px-3">
                          <div className="font-medium">{doc.customer_name}</div>
                          <div className="text-xs text-gray-500">{doc.account}</div>
                        </td>
                        <td className="py-2 px-3">{doc.document_date || '-'}</td>
                        <td className="py-2 px-3">
                          <span className={`px-2 py-0.5 rounded text-xs font-medium ${getStatusColor(doc.status)}`}>
                            {doc.status_desc}
                          </span>
                        </td>
                        <td className="py-2 px-3 text-right">{formatCurrency(doc.net_value)}</td>
                        <td className="py-2 px-3 text-right font-medium">{formatCurrency(doc.gross_value)}</td>
                        <td className="py-2 px-3">
                          <ChevronRight className="h-4 w-4 text-gray-400" />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

        {/* Right Panel - Document Detail */}
        {selectedDoc && (
          <div className="w-[450px]">
            <div className="card">
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="font-semibold text-lg">{documentDetail?.header.document || selectedDoc}</h3>
                  <p className="text-gray-600 text-sm">{documentDetail?.header.customer_name}</p>
                </div>
                <button onClick={() => setSelectedDoc(null)} className="text-gray-400 hover:text-gray-600">
                  <X className="h-5 w-5" />
                </button>
              </div>

              {detailLoading ? (
                <div className="text-center py-8 text-gray-500">Loading...</div>
              ) : documentDetail ? (
                <div className="space-y-4">
                  {/* Summary */}
                  <div className="grid grid-cols-3 gap-3">
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <div className="text-lg font-bold">{formatCurrency(documentDetail.header.net_value)}</div>
                      <div className="text-xs text-gray-500">Net</div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <div className="text-lg font-bold">{formatCurrency(documentDetail.header.vat_value)}</div>
                      <div className="text-xs text-gray-500">VAT</div>
                    </div>
                    <div className="bg-blue-50 rounded-lg p-3 text-center">
                      <div className="text-lg font-bold text-blue-700">{formatCurrency(documentDetail.header.gross_value)}</div>
                      <div className="text-xs text-blue-600">Gross</div>
                    </div>
                  </div>

                  {/* Document Info */}
                  <div className="text-sm space-y-2">
                    <div className="flex justify-between py-1 border-b border-gray-100">
                      <span className="text-gray-500">Account</span>
                      <span className="font-medium">{documentDetail.header.account}</span>
                    </div>
                    {documentDetail.header.customer_ref && (
                      <div className="flex justify-between py-1 border-b border-gray-100">
                        <span className="text-gray-500">Customer Ref</span>
                        <span className="font-medium">{documentDetail.header.customer_ref}</span>
                      </div>
                    )}
                    {documentDetail.header.sales_order && (
                      <div className="flex justify-between py-1 border-b border-gray-100">
                        <span className="text-gray-500">Sales Order</span>
                        <span className="font-medium">{documentDetail.header.sales_order}</span>
                      </div>
                    )}
                    {documentDetail.header.invoice && (
                      <div className="flex justify-between py-1 border-b border-gray-100">
                        <span className="text-gray-500">Invoice</span>
                        <span className="font-medium">{documentDetail.header.invoice}</span>
                      </div>
                    )}
                    <div className="flex justify-between py-1 border-b border-gray-100">
                      <span className="text-gray-500">Warehouse</span>
                      <span className="font-medium">{documentDetail.header.warehouse || '-'}</span>
                    </div>
                  </div>

                  {/* Lines */}
                  <div>
                    <h4 className="font-medium mb-2">Lines ({documentDetail.lines.length})</h4>
                    <div className="space-y-2 max-h-64 overflow-y-auto">
                      {documentDetail.lines.map((line) => (
                        <div key={line.line_number} className="border border-gray-200 rounded-lg p-2 text-sm">
                          <div className="flex justify-between items-start">
                            <div>
                              <span className="font-mono text-blue-600">{line.stock_ref}</span>
                              <p className="text-gray-600 text-xs">{line.description}</p>
                            </div>
                            <span className="font-medium">{formatCurrency(line.line_value)}</span>
                          </div>
                          <div className="flex gap-4 mt-1 text-xs text-gray-500">
                            <span>Qty: {line.quantity}</span>
                            <span>@ {formatCurrency(line.unit_price)}</span>
                            {line.discount_percent > 0 && <span>-{line.discount_percent}%</span>}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              ) : null}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
