import { useState, useEffect } from 'react';
import { authFetch } from '../api/client';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ShoppingCart, ChevronRight, X, Filter, Plus, ArrowRight, Package, FileText } from 'lucide-react';

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

interface Customer {
  account: string;
  name: string;
  address1: string;
  postcode: string;
  balance: number;
}

interface LineItem {
  stock_ref: string;
  description: string;
  quantity: number;
  price: number;
  vat_code: string;
}

async function fetchDocuments(params: { status?: string; account?: string; limit?: number; offset?: number }) {
  const queryParams = new URLSearchParams();
  if (params.status) queryParams.set('status', params.status);
  if (params.account) queryParams.set('account', params.account);
  queryParams.set('limit', String(params.limit || 50));
  queryParams.set('offset', String(params.offset || 0));

  const response = await authFetch(`${API_BASE}/api/sop/documents?${queryParams}`);
  if (!response.ok) throw new Error('Failed to fetch documents');
  return response.json();
}

async function fetchDocumentDetail(docNumber: string): Promise<SOPDetail> {
  const response = await authFetch(`${API_BASE}/api/sop/documents/${encodeURIComponent(docNumber)}`);
  if (!response.ok) throw new Error('Failed to fetch document detail');
  return response.json();
}

async function fetchCustomers(search: string): Promise<Customer[]> {
  const response = await authFetch(`${API_BASE}/api/sop/customers?search=${encodeURIComponent(search)}&limit=20`);
  if (!response.ok) throw new Error('Failed to fetch customers');
  const data = await response.json();
  return data.customers || [];
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
  const queryClient = useQueryClient();
  const [selectedStatus, setSelectedStatus] = useState('');
  const [accountFilter, setAccountFilter] = useState('');
  const [selectedDoc, setSelectedDoc] = useState<string | null>(null);
  const [showFilters, setShowFilters] = useState(false);
  const [page, setPage] = useState(0);
  const pageSize = 50;

  // Modal states
  const [showQuoteModal, setShowQuoteModal] = useState(false);
  const [showOrderModal, setShowOrderModal] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState('');
  const [submitSuccess, setSubmitSuccess] = useState('');

  // Form states
  const [customerSearch, setCustomerSearch] = useState('');
  const [debouncedCustomerSearch, setDebouncedCustomerSearch] = useState('');
  const [selectedCustomer, setSelectedCustomer] = useState<Customer | null>(null);
  const [customerRef, setCustomerRef] = useState('');
  const [warehouse, setWarehouse] = useState('MAIN');
  const [notes, setNotes] = useState('');
  const [autoAllocate, setAutoAllocate] = useState(false);
  const [lines, setLines] = useState<LineItem[]>([{ stock_ref: '', description: '', quantity: 1, price: 0, vat_code: 'S' }]);

  // Debounce customer search - only search after 300ms of no typing
  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedCustomerSearch(customerSearch);
    }, 300);
    return () => clearTimeout(timer);
  }, [customerSearch]);

  // Documents list - cached for 1 minute
  const { data: documentsData, isLoading, refetch } = useQuery({
    queryKey: ['sop-documents', selectedStatus, accountFilter, page],
    queryFn: () => fetchDocuments({
      status: selectedStatus,
      account: accountFilter,
      limit: pageSize,
      offset: page * pageSize,
    }),
    staleTime: 60000,
    gcTime: 2 * 60 * 1000,
  });

  // Document detail - cached for 2 minutes
  const { data: documentDetail, isLoading: detailLoading } = useQuery({
    queryKey: ['sop-document-detail', selectedDoc],
    queryFn: () => fetchDocumentDetail(selectedDoc!),
    enabled: !!selectedDoc,
    staleTime: 2 * 60 * 1000,
    gcTime: 5 * 60 * 1000,
  });

  // Customer search - uses debounced query, cached for 30s
  const { data: customers } = useQuery({
    queryKey: ['sop-customers', debouncedCustomerSearch],
    queryFn: () => fetchCustomers(debouncedCustomerSearch),
    enabled: debouncedCustomerSearch.length >= 2,
    staleTime: 30000,
    gcTime: 60000,
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

  const resetForm = () => {
    setCustomerSearch('');
    setSelectedCustomer(null);
    setCustomerRef('');
    setWarehouse('MAIN');
    setNotes('');
    setAutoAllocate(false);
    setLines([{ stock_ref: '', description: '', quantity: 1, price: 0, vat_code: 'S' }]);
    setSubmitError('');
    setSubmitSuccess('');
  };

  const addLine = () => {
    setLines([...lines, { stock_ref: '', description: '', quantity: 1, price: 0, vat_code: 'S' }]);
  };

  const updateLine = (index: number, field: keyof LineItem, value: string | number) => {
    const newLines = [...lines];
    newLines[index] = { ...newLines[index], [field]: value };
    setLines(newLines);
  };

  const removeLine = (index: number) => {
    if (lines.length > 1) {
      setLines(lines.filter((_, i) => i !== index));
    }
  };

  const calculateTotal = () => {
    return lines.reduce((sum, line) => sum + (line.quantity * line.price), 0);
  };

  const handleCreateQuote = async () => {
    if (!selectedCustomer) {
      setSubmitError('Please select a customer');
      return;
    }

    const validLines = lines.filter(l => l.description.trim() && l.quantity > 0 && l.price > 0);
    if (validLines.length === 0) {
      setSubmitError('Please add at least one line item with description, quantity, and price');
      return;
    }

    setIsSubmitting(true);
    setSubmitError('');
    setSubmitSuccess('');

    try {
      const params = new URLSearchParams({
        customer_account: selectedCustomer.account,
        customer_ref: customerRef,
        warehouse: warehouse,
        notes: notes,
        lines: JSON.stringify(validLines),
      });

      const response = await authFetch(`${API_BASE}/api/sop/quotes?${params}`, { method: 'POST' });
      const result = await response.json();

      if (result.success) {
        setSubmitSuccess(`Quote ${result.quote_number} created successfully`);
        setTimeout(() => {
          setShowQuoteModal(false);
          resetForm();
          refetch();
        }, 1500);
      } else {
        setSubmitError(result.error || 'Failed to create quote');
      }
    } catch (err) {
      setSubmitError(String(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleCreateOrder = async () => {
    if (!selectedCustomer) {
      setSubmitError('Please select a customer');
      return;
    }

    const validLines = lines.filter(l => l.description.trim() && l.quantity > 0 && l.price > 0);
    if (validLines.length === 0) {
      setSubmitError('Please add at least one line item');
      return;
    }

    setIsSubmitting(true);
    setSubmitError('');
    setSubmitSuccess('');

    try {
      const params = new URLSearchParams({
        customer_account: selectedCustomer.account,
        customer_ref: customerRef,
        warehouse: warehouse,
        auto_allocate: String(autoAllocate),
        notes: notes,
        lines: JSON.stringify(validLines),
      });

      const response = await authFetch(`${API_BASE}/api/sop/orders?${params}`, { method: 'POST' });
      const result = await response.json();

      if (result.success) {
        setSubmitSuccess(`Order ${result.order_number} created successfully`);
        setTimeout(() => {
          setShowOrderModal(false);
          resetForm();
          refetch();
        }, 1500);
      } else {
        setSubmitError(result.error || 'Failed to create order');
      }
    } catch (err) {
      setSubmitError(String(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleConvertQuote = async () => {
    if (!selectedDoc || !documentDetail) return;

    setIsSubmitting(true);
    setSubmitError('');

    try {
      const response = await authFetch(`${API_BASE}/api/sop/quotes/${encodeURIComponent(selectedDoc)}/convert`, { method: 'POST' });
      const result = await response.json();

      if (result.success) {
        queryClient.invalidateQueries({ queryKey: ['sop-documents'] });
        queryClient.invalidateQueries({ queryKey: ['sop-document-detail', selectedDoc] });
        setSelectedDoc(null);
        refetch();
      } else {
        setSubmitError(result.error || 'Failed to convert quote');
      }
    } catch (err) {
      setSubmitError(String(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleAllocateStock = async () => {
    if (!selectedDoc) return;

    setIsSubmitting(true);
    setSubmitError('');

    try {
      const response = await authFetch(`${API_BASE}/api/sop/orders/${encodeURIComponent(selectedDoc)}/allocate`, { method: 'POST' });
      const result = await response.json();

      if (result.success) {
        queryClient.invalidateQueries({ queryKey: ['sop-document-detail', selectedDoc] });
      } else {
        setSubmitError(result.error || 'Failed to allocate stock');
      }
    } catch (err) {
      setSubmitError(String(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleCreateInvoice = async () => {
    if (!selectedDoc) return;

    setIsSubmitting(true);
    setSubmitError('');

    try {
      const response = await authFetch(`${API_BASE}/api/sop/orders/${encodeURIComponent(selectedDoc)}/invoice`, { method: 'POST' });
      const result = await response.json();

      if (result.success) {
        setSubmitSuccess(`Invoice ${result.invoice_number} created successfully`);
        queryClient.invalidateQueries({ queryKey: ['sop-documents'] });
        queryClient.invalidateQueries({ queryKey: ['sop-document-detail', selectedDoc] });
        setTimeout(() => {
          setSelectedDoc(null);
          setSubmitSuccess('');
          refetch();
        }, 2000);
      } else {
        setSubmitError(result.error || 'Failed to create invoice');
      }
    } catch (err) {
      setSubmitError(String(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  // Quote/Order Modal Component
  const DocumentModal = ({ isQuote }: { isQuote: boolean }) => (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-3xl max-h-[90vh] overflow-y-auto">
        <div className="p-6 border-b border-gray-200">
          <div className="flex justify-between items-center">
            <h3 className="text-lg font-semibold">
              {isQuote ? 'Create New Quote' : 'Create New Order'}
            </h3>
            <button
              onClick={() => { isQuote ? setShowQuoteModal(false) : setShowOrderModal(false); resetForm(); }}
              className="text-gray-400 hover:text-gray-600"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>

        <div className="p-6 space-y-6">
          {/* Customer Selection */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Customer *</label>
            {selectedCustomer ? (
              <div className="flex items-center justify-between p-3 bg-blue-50 border border-blue-200 rounded-lg">
                <div>
                  <div className="font-medium">{selectedCustomer.name}</div>
                  <div className="text-sm text-gray-600">{selectedCustomer.account} • {selectedCustomer.postcode}</div>
                </div>
                <button
                  onClick={() => setSelectedCustomer(null)}
                  className="text-blue-600 hover:text-blue-800 text-sm"
                >
                  Change
                </button>
              </div>
            ) : (
              <div className="relative">
                <input
                  type="text"
                  value={customerSearch}
                  onChange={(e) => setCustomerSearch(e.target.value)}
                  placeholder="Search by account or name..."
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                />
                {customers && customers.length > 0 && (
                  <div className="absolute z-10 w-full mt-1 bg-white border border-gray-200 rounded-lg shadow-lg max-h-48 overflow-y-auto">
                    {customers.map((c) => (
                      <button
                        key={c.account}
                        onClick={() => { setSelectedCustomer(c); setCustomerSearch(''); }}
                        className="w-full text-left px-3 py-2 hover:bg-gray-50"
                      >
                        <div className="font-medium">{c.name}</div>
                        <div className="text-sm text-gray-500">{c.account} • {c.postcode}</div>
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="grid grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Customer Ref</label>
              <input
                type="text"
                value={customerRef}
                onChange={(e) => setCustomerRef(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                placeholder="PO number, etc."
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Warehouse</label>
              <input
                type="text"
                value={warehouse}
                onChange={(e) => setWarehouse(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg"
              />
            </div>
            {!isQuote && (
              <div className="flex items-end">
                <label className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    checked={autoAllocate}
                    onChange={(e) => setAutoAllocate(e.target.checked)}
                    className="w-4 h-4"
                  />
                  <span className="text-sm">Auto-allocate stock</span>
                </label>
              </div>
            )}
          </div>

          {/* Line Items */}
          <div>
            <div className="flex justify-between items-center mb-2">
              <label className="block text-sm font-medium text-gray-700">Line Items</label>
              <button
                onClick={addLine}
                className="text-blue-600 hover:text-blue-800 text-sm flex items-center gap-1"
              >
                <Plus className="h-4 w-4" /> Add Line
              </button>
            </div>
            <div className="space-y-2">
              {lines.map((line, idx) => (
                <div key={idx} className="grid grid-cols-12 gap-2 items-center">
                  <input
                    type="text"
                    value={line.stock_ref}
                    onChange={(e) => updateLine(idx, 'stock_ref', e.target.value)}
                    placeholder="Stock Ref"
                    className="col-span-2 px-2 py-1 border border-gray-300 rounded text-sm"
                  />
                  <input
                    type="text"
                    value={line.description}
                    onChange={(e) => updateLine(idx, 'description', e.target.value)}
                    placeholder="Description *"
                    className="col-span-4 px-2 py-1 border border-gray-300 rounded text-sm"
                  />
                  <input
                    type="number"
                    value={line.quantity}
                    onChange={(e) => updateLine(idx, 'quantity', parseFloat(e.target.value) || 0)}
                    placeholder="Qty"
                    className="col-span-1 px-2 py-1 border border-gray-300 rounded text-sm text-right"
                  />
                  <input
                    type="number"
                    value={line.price}
                    onChange={(e) => updateLine(idx, 'price', parseFloat(e.target.value) || 0)}
                    placeholder="Price"
                    step="0.01"
                    className="col-span-2 px-2 py-1 border border-gray-300 rounded text-sm text-right"
                  />
                  <select
                    value={line.vat_code}
                    onChange={(e) => updateLine(idx, 'vat_code', e.target.value)}
                    className="col-span-2 px-2 py-1 border border-gray-300 rounded text-sm"
                  >
                    <option value="S">Standard (20%)</option>
                    <option value="R">Reduced (5%)</option>
                    <option value="Z">Zero</option>
                    <option value="E">Exempt</option>
                  </select>
                  <button
                    onClick={() => removeLine(idx)}
                    className="col-span-1 text-gray-400 hover:text-red-600"
                    disabled={lines.length === 1}
                  >
                    <X className="h-4 w-4" />
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* Notes */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Notes</label>
            <textarea
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              rows={2}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg"
            />
          </div>

          {/* Total */}
          <div className="flex justify-end">
            <div className="text-right">
              <div className="text-sm text-gray-500">Net Total</div>
              <div className="text-2xl font-bold">{formatCurrency(calculateTotal())}</div>
            </div>
          </div>

          {/* Error/Success Messages */}
          {submitError && (
            <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
              {submitError}
            </div>
          )}
          {submitSuccess && (
            <div className="p-3 bg-green-50 border border-green-200 rounded-lg text-green-700 text-sm">
              {submitSuccess}
            </div>
          )}
        </div>

        <div className="p-6 border-t border-gray-200 flex justify-end gap-3">
          <button
            onClick={() => { isQuote ? setShowQuoteModal(false) : setShowOrderModal(false); resetForm(); }}
            className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50"
          >
            Cancel
          </button>
          <button
            onClick={isQuote ? handleCreateQuote : handleCreateOrder}
            disabled={isSubmitting}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            {isSubmitting ? 'Creating...' : (isQuote ? 'Create Quote' : 'Create Order')}
          </button>
        </div>
      </div>
    </div>
  );

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-start">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <ShoppingCart className="h-7 w-7 text-blue-600" />
            Sales Order Processing
          </h2>
          <p className="text-gray-600 mt-1">View and manage sales documents - quotes, orders, deliveries, invoices</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={() => setShowQuoteModal(true)}
            className="px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 flex items-center gap-2"
          >
            <Plus className="h-4 w-4" />
            New Quote
          </button>
          <button
            onClick={() => setShowOrderModal(true)}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
          >
            <Plus className="h-4 w-4" />
            New Order
          </button>
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

              {/* Action Buttons based on status */}
              {documentDetail && (
                <div className="flex gap-2 mb-4">
                  {documentDetail.header.status === 'Q' && (
                    <button
                      onClick={handleConvertQuote}
                      disabled={isSubmitting}
                      className="flex-1 px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center justify-center gap-2 text-sm"
                    >
                      <ArrowRight className="h-4 w-4" />
                      Convert to Order
                    </button>
                  )}
                  {documentDetail.header.status === 'O' && (
                    <>
                      <button
                        onClick={handleAllocateStock}
                        disabled={isSubmitting}
                        className="flex-1 px-3 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 flex items-center justify-center gap-2 text-sm"
                      >
                        <Package className="h-4 w-4" />
                        Allocate
                      </button>
                      <button
                        onClick={handleCreateInvoice}
                        disabled={isSubmitting}
                        className="flex-1 px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center justify-center gap-2 text-sm"
                      >
                        <FileText className="h-4 w-4" />
                        Invoice
                      </button>
                    </>
                  )}
                </div>
              )}

              {submitError && (
                <div className="mb-4 p-2 bg-red-50 border border-red-200 rounded text-red-700 text-sm">
                  {submitError}
                </div>
              )}
              {submitSuccess && (
                <div className="mb-4 p-2 bg-green-50 border border-green-200 rounded text-green-700 text-sm">
                  {submitSuccess}
                </div>
              )}

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

      {/* Modals */}
      {showQuoteModal && <DocumentModal isQuote={true} />}
      {showOrderModal && <DocumentModal isQuote={false} />}
    </div>
  );
}
