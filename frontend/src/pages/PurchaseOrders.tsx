import { useState, useEffect } from 'react';
import { authFetch } from '../api/client';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { Truck, ChevronRight, X, Filter, Plus, Package, FileCheck } from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

interface PurchaseOrder {
  po_number: string;
  supplier_account: string;
  supplier_name: string;
  total_value: number;
  overall_discount: number;
  warehouse: string;
  is_cancelled: boolean;
  is_printed: boolean;
  currency: string;
  created_date: string | null;
  reference: string;
}

interface POLine {
  line_number: number;
  stock_ref: string;
  supplier_ref: string;
  description: string;
  quantity: number;
  unit_price: number;
  line_value: number;
  discount_percent: number;
  warehouse: string;
  required_date: string | null;
  ledger_account: string;
  job_number: string;
  quantity_ordered?: number;
  quantity_received?: number;
  quantity_outstanding?: number;
}

interface PODetail {
  header: PurchaseOrder & {
    delivery_name: string;
    delivery_address_1: string;
    delivery_address_2: string;
    delivery_address_3: string;
    delivery_address_4: string;
    delivery_postcode: string;
    contact: string;
    exchange_rate: number;
    narrative_1: string;
    narrative_2: string;
  };
  lines: POLine[];
}

interface GRN {
  grn_number: string;
  grn_date: string | null;
  delivery_ref: string;
  delivery_charge: number;
  vat_on_delivery: number;
  received_by: string;
  status: string;
  created_date: string | null;
}

interface Supplier {
  account: string;
  name: string;
  address1: string;
  postcode: string;
  phone: string;
}

interface POLineItem {
  stock_ref: string;
  supplier_ref: string;
  description: string;
  quantity: number;
  unit_price: number;
  discount_percent: number;
  warehouse: string;
}

async function fetchOrders(params: { status?: string; account?: string; limit?: number; offset?: number }) {
  const queryParams = new URLSearchParams();
  if (params.status) queryParams.set('status', params.status);
  if (params.account) queryParams.set('account', params.account);
  queryParams.set('limit', String(params.limit || 50));
  queryParams.set('offset', String(params.offset || 0));

  const response = await authFetch(`${API_BASE}/api/pop/orders?${queryParams}`);
  if (!response.ok) throw new Error('Failed to fetch orders');
  return response.json();
}

async function fetchOrderDetail(poNumber: string): Promise<PODetail> {
  const response = await authFetch(`${API_BASE}/api/pop/orders/${encodeURIComponent(poNumber)}`);
  if (!response.ok) throw new Error('Failed to fetch order detail');
  return response.json();
}

async function fetchOutstanding(poNumber: string) {
  const response = await authFetch(`${API_BASE}/api/pop/orders/${encodeURIComponent(poNumber)}/outstanding`);
  if (!response.ok) throw new Error('Failed to fetch outstanding');
  return response.json();
}

async function fetchGRNs(params: { limit?: number; offset?: number }) {
  const queryParams = new URLSearchParams();
  queryParams.set('limit', String(params.limit || 50));
  queryParams.set('offset', String(params.offset || 0));

  const response = await authFetch(`${API_BASE}/api/pop/grns?${queryParams}`);
  if (!response.ok) throw new Error('Failed to fetch GRNs');
  return response.json();
}

async function searchSuppliers(term: string): Promise<Supplier[]> {
  if (!term || term.length < 2) return [];
  const response = await authFetch(`${API_BASE}/api/pop/suppliers?search=${encodeURIComponent(term)}`);
  if (!response.ok) return [];
  const data = await response.json();
  return data.suppliers || [];
}

function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  return new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(value);
}

export function PurchaseOrders() {
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState<'orders' | 'grns'>('orders');
  const [statusFilter, setStatusFilter] = useState('open');
  const [accountFilter, setAccountFilter] = useState('');
  const [selectedPO, setSelectedPO] = useState<string | null>(null);
  const [showFilters, setShowFilters] = useState(false);
  const [page, setPage] = useState(0);
  const pageSize = 50;

  // Modal states
  const [showCreatePOModal, setShowCreatePOModal] = useState(false);
  const [showReceiveModal, setShowReceiveModal] = useState(false);

  // Create PO form state
  const [supplierSearch, setSupplierSearch] = useState('');
  const [supplierResults, setSupplierResults] = useState<Supplier[]>([]);
  const [selectedSupplier, setSelectedSupplier] = useState<Supplier | null>(null);
  const [poReference, setPOReference] = useState('');
  const [poNarrative, setPONarrative] = useState('');
  const [poWarehouse, setPOWarehouse] = useState('MAIN');
  const [poLines, setPOLines] = useState<POLineItem[]>([
    { stock_ref: '', supplier_ref: '', description: '', quantity: 1, unit_price: 0, discount_percent: 0, warehouse: 'MAIN' }
  ]);

  // Receive modal state
  const [outstandingLines, setOutstandingLines] = useState<POLine[]>([]);
  const [receiveQuantities, setReceiveQuantities] = useState<{ [key: number]: number }>({});
  const [deliveryRef, setDeliveryRef] = useState('');

  // Status messages
  const [actionStatus, setActionStatus] = useState<{ type: 'success' | 'error'; message: string } | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Supplier search effect
  useEffect(() => {
    const timer = setTimeout(async () => {
      if (supplierSearch.length >= 2) {
        const results = await searchSuppliers(supplierSearch);
        setSupplierResults(results);
      } else {
        setSupplierResults([]);
      }
    }, 300);
    return () => clearTimeout(timer);
  }, [supplierSearch]);

  const { data: ordersData, isLoading: ordersLoading } = useQuery({
    queryKey: ['pop-orders', statusFilter, accountFilter, page],
    queryFn: () => fetchOrders({
      status: statusFilter,
      account: accountFilter,
      limit: pageSize,
      offset: page * pageSize,
    }),
    enabled: activeTab === 'orders',
  });

  const { data: grnsData, isLoading: grnsLoading } = useQuery({
    queryKey: ['pop-grns', page],
    queryFn: () => fetchGRNs({ limit: pageSize, offset: page * pageSize }),
    enabled: activeTab === 'grns',
  });

  const { data: poDetail, isLoading: detailLoading } = useQuery({
    queryKey: ['pop-order-detail', selectedPO],
    queryFn: () => fetchOrderDetail(selectedPO!),
    enabled: !!selectedPO,
  });

  const orders: PurchaseOrder[] = ordersData?.orders || [];
  const grns: GRN[] = grnsData?.grns || [];
  const totalOrders = ordersData?.count || 0;
  const totalGRNs = grnsData?.count || 0;
  const totalPages = Math.ceil((activeTab === 'orders' ? totalOrders : totalGRNs) / pageSize);

  // Add a new line item
  const addLine = () => {
    setPOLines([...poLines, { stock_ref: '', supplier_ref: '', description: '', quantity: 1, unit_price: 0, discount_percent: 0, warehouse: poWarehouse }]);
  };

  // Remove a line item
  const removeLine = (index: number) => {
    if (poLines.length > 1) {
      setPOLines(poLines.filter((_, i) => i !== index));
    }
  };

  // Update a line item
  const updateLine = (index: number, field: keyof POLineItem, value: string | number) => {
    const updated = [...poLines];
    updated[index] = { ...updated[index], [field]: value };
    setPOLines(updated);
  };

  // Calculate PO total
  const poTotal = poLines.reduce((sum, line) => {
    const lineValue = line.quantity * line.unit_price * (1 - line.discount_percent / 100);
    return sum + lineValue;
  }, 0);

  // Create PO handler
  const handleCreatePO = async () => {
    if (!selectedSupplier) {
      setActionStatus({ type: 'error', message: 'Please select a supplier' });
      return;
    }

    const validLines = poLines.filter(l => l.description && l.quantity > 0);
    if (validLines.length === 0) {
      setActionStatus({ type: 'error', message: 'At least one line with description and quantity is required' });
      return;
    }

    setIsSubmitting(true);
    setActionStatus(null);

    try {
      const params = new URLSearchParams({
        supplier_account: selectedSupplier.account,
        lines: JSON.stringify(validLines),
        warehouse: poWarehouse,
        reference: poReference,
        narrative: poNarrative,
      });

      const response = await authFetch(`${API_BASE}/api/pop/orders?${params}`, { method: 'POST' });
      const result = await response.json();

      if (!response.ok) {
        throw new Error(result.detail || 'Failed to create PO');
      }

      setActionStatus({ type: 'success', message: `Purchase Order ${result.po_number} created successfully` });
      queryClient.invalidateQueries({ queryKey: ['pop-orders'] });

      // Reset form after delay
      setTimeout(() => {
        setShowCreatePOModal(false);
        resetPOForm();
      }, 2000);
    } catch (error) {
      setActionStatus({ type: 'error', message: error instanceof Error ? error.message : 'Failed to create PO' });
    } finally {
      setIsSubmitting(false);
    }
  };

  // Reset PO form
  const resetPOForm = () => {
    setSelectedSupplier(null);
    setSupplierSearch('');
    setSupplierResults([]);
    setPOReference('');
    setPONarrative('');
    setPOWarehouse('MAIN');
    setPOLines([{ stock_ref: '', supplier_ref: '', description: '', quantity: 1, unit_price: 0, discount_percent: 0, warehouse: 'MAIN' }]);
    setActionStatus(null);
  };

  // Open receive modal
  const openReceiveModal = async () => {
    if (!selectedPO) return;
    setIsSubmitting(true);
    try {
      const data = await fetchOutstanding(selectedPO);
      setOutstandingLines(data.outstanding_lines || []);
      const initialQtys: { [key: number]: number } = {};
      (data.outstanding_lines || []).forEach((line: POLine) => {
        initialQtys[line.line_number] = line.quantity_outstanding || 0;
      });
      setReceiveQuantities(initialQtys);
      setDeliveryRef('');
      setShowReceiveModal(true);
    } catch (error) {
      setActionStatus({ type: 'error', message: 'Failed to load outstanding lines' });
    } finally {
      setIsSubmitting(false);
    }
  };

  // Receive goods handler
  const handleReceiveGoods = async () => {
    if (!selectedPO) return;

    const linesToReceive = outstandingLines
      .filter(l => (receiveQuantities[l.line_number] || 0) > 0)
      .map(l => ({
        line_number: l.line_number,
        quantity: receiveQuantities[l.line_number] || 0,
      }));

    if (linesToReceive.length === 0) {
      setActionStatus({ type: 'error', message: 'No quantities to receive' });
      return;
    }

    setIsSubmitting(true);
    setActionStatus(null);

    try {
      const params = new URLSearchParams({
        lines: JSON.stringify(linesToReceive),
        delivery_ref: deliveryRef,
      });

      const response = await authFetch(`${API_BASE}/api/pop/orders/${encodeURIComponent(selectedPO)}/receive?${params}`, {
        method: 'POST'
      });
      const result = await response.json();

      if (!response.ok) {
        throw new Error(result.detail || 'Failed to receive goods');
      }

      setActionStatus({ type: 'success', message: `GRN ${result.grn_number} created successfully` });
      queryClient.invalidateQueries({ queryKey: ['pop-orders'] });
      queryClient.invalidateQueries({ queryKey: ['pop-grns'] });
      queryClient.invalidateQueries({ queryKey: ['pop-order-detail', selectedPO] });

      setTimeout(() => {
        setShowReceiveModal(false);
        setActionStatus(null);
      }, 2000);
    } catch (error) {
      setActionStatus({ type: 'error', message: error instanceof Error ? error.message : 'Failed to receive goods' });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-start">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Truck className="h-7 w-7 text-blue-600" />
            Purchase Order Processing
          </h2>
          <p className="text-gray-600 mt-1">Manage purchase orders and goods received notes</p>
        </div>
        <button
          onClick={() => { setShowCreatePOModal(true); resetPOForm(); }}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
        >
          <Plus className="h-4 w-4" />
          New Purchase Order
        </button>
      </div>

      {/* Status Message */}
      {actionStatus && !showCreatePOModal && !showReceiveModal && (
        <div className={`p-4 rounded-lg ${actionStatus.type === 'success' ? 'bg-green-50 text-green-800 border border-green-200' : 'bg-red-50 text-red-800 border border-red-200'}`}>
          {actionStatus.message}
          <button onClick={() => setActionStatus(null)} className="ml-4 text-sm underline">Dismiss</button>
        </div>
      )}

      <div className="flex gap-6">
        {/* Left Panel */}
        <div className="flex-1 space-y-4">
          {/* Tab Buttons */}
          <div className="card">
            <div className="flex gap-4 items-center">
              <div className="flex gap-2">
                <button
                  onClick={() => { setActiveTab('orders'); setPage(0); setSelectedPO(null); }}
                  className={`px-4 py-2 rounded-lg font-medium transition-colors ${
                    activeTab === 'orders' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  Purchase Orders
                </button>
                <button
                  onClick={() => { setActiveTab('grns'); setPage(0); setSelectedPO(null); }}
                  className={`px-4 py-2 rounded-lg font-medium transition-colors ${
                    activeTab === 'grns' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  GRNs
                </button>
              </div>

              {activeTab === 'orders' && (
                <>
                  <div className="flex gap-2 ml-4">
                    {[{ value: 'open', label: 'Open' }, { value: 'all', label: 'All' }, { value: 'cancelled', label: 'Cancelled' }].map((s) => (
                      <button
                        key={s.value}
                        onClick={() => { setStatusFilter(s.value); setPage(0); }}
                        className={`px-3 py-1 rounded text-sm ${
                          statusFilter === s.value ? 'bg-blue-100 text-blue-700' : 'text-gray-600 hover:bg-gray-100'
                        }`}
                      >
                        {s.label}
                      </button>
                    ))}
                  </div>
                  <button
                    onClick={() => setShowFilters(!showFilters)}
                    className={`px-3 py-1.5 border rounded text-sm flex items-center gap-1 ${
                      showFilters || accountFilter ? 'bg-blue-50 border-blue-300' : 'border-gray-300'
                    }`}
                  >
                    <Filter className="h-4 w-4" />
                  </button>
                </>
              )}
            </div>

            {showFilters && activeTab === 'orders' && (
              <div className="mt-4 pt-4 border-t border-gray-200">
                <div className="flex gap-4 items-end">
                  <div className="flex-1">
                    <label className="block text-sm font-medium text-gray-700 mb-1">Supplier Account</label>
                    <input
                      type="text"
                      value={accountFilter}
                      onChange={(e) => setAccountFilter(e.target.value)}
                      placeholder="e.g. SUP001"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                    />
                  </div>
                  <button onClick={() => setAccountFilter('')} className="px-3 py-2 text-sm text-gray-600">
                    Clear
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* Orders Table */}
          {activeTab === 'orders' && (
            <div className="card">
              <div className="flex justify-between items-center mb-4">
                <h3 className="font-semibold">
                  Purchase Orders {totalOrders > 0 && <span className="text-gray-500 font-normal">({totalOrders})</span>}
                </h3>
                {totalPages > 1 && (
                  <div className="flex items-center gap-2 text-sm">
                    <button onClick={() => setPage(Math.max(0, page - 1))} disabled={page === 0} className="px-2 py-1 border rounded disabled:opacity-50">Previous</button>
                    <span>Page {page + 1} of {totalPages}</span>
                    <button onClick={() => setPage(Math.min(totalPages - 1, page + 1))} disabled={page >= totalPages - 1} className="px-2 py-1 border rounded disabled:opacity-50">Next</button>
                  </div>
                )}
              </div>

              {ordersLoading ? (
                <div className="text-center py-8 text-gray-500">Loading...</div>
              ) : orders.length === 0 ? (
                <div className="text-center py-8 text-gray-500">No purchase orders found</div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-200">
                        <th className="text-left py-2 px-3 font-medium">PO Number</th>
                        <th className="text-left py-2 px-3 font-medium">Supplier</th>
                        <th className="text-left py-2 px-3 font-medium">Date</th>
                        <th className="text-right py-2 px-3 font-medium">Value</th>
                        <th className="text-left py-2 px-3 font-medium">Status</th>
                        <th className="w-8"></th>
                      </tr>
                    </thead>
                    <tbody>
                      {orders.map((order) => (
                        <tr
                          key={order.po_number}
                          onClick={() => setSelectedPO(order.po_number)}
                          className={`border-b border-gray-100 cursor-pointer transition-colors ${
                            selectedPO === order.po_number ? 'bg-blue-50' : 'hover:bg-gray-50'
                          }`}
                        >
                          <td className="py-2 px-3 font-mono text-blue-600">{order.po_number}</td>
                          <td className="py-2 px-3">
                            <div className="font-medium">{order.supplier_name || '-'}</div>
                            <div className="text-xs text-gray-500">{order.supplier_account}</div>
                          </td>
                          <td className="py-2 px-3">{order.created_date || '-'}</td>
                          <td className="py-2 px-3 text-right font-medium">{formatCurrency(order.total_value)}</td>
                          <td className="py-2 px-3">
                            {order.is_cancelled ? (
                              <span className="px-2 py-0.5 bg-red-100 text-red-700 rounded text-xs">Cancelled</span>
                            ) : order.is_printed ? (
                              <span className="px-2 py-0.5 bg-green-100 text-green-700 rounded text-xs">Printed</span>
                            ) : (
                              <span className="px-2 py-0.5 bg-blue-100 text-blue-700 rounded text-xs">Open</span>
                            )}
                          </td>
                          <td className="py-2 px-3"><ChevronRight className="h-4 w-4 text-gray-400" /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}

          {/* GRNs Table */}
          {activeTab === 'grns' && (
            <div className="card">
              <div className="flex justify-between items-center mb-4">
                <h3 className="font-semibold">
                  Goods Received Notes {totalGRNs > 0 && <span className="text-gray-500 font-normal">({totalGRNs})</span>}
                </h3>
              </div>

              {grnsLoading ? (
                <div className="text-center py-8 text-gray-500">Loading...</div>
              ) : grns.length === 0 ? (
                <div className="text-center py-8 text-gray-500">No GRNs found</div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-200">
                        <th className="text-left py-2 px-3 font-medium">GRN Number</th>
                        <th className="text-left py-2 px-3 font-medium">Date</th>
                        <th className="text-left py-2 px-3 font-medium">Delivery Ref</th>
                        <th className="text-left py-2 px-3 font-medium">Received By</th>
                        <th className="text-right py-2 px-3 font-medium">Del. Charge</th>
                      </tr>
                    </thead>
                    <tbody>
                      {grns.map((grn) => (
                        <tr key={grn.grn_number} className="border-b border-gray-100 hover:bg-gray-50">
                          <td className="py-2 px-3 font-mono text-blue-600">{grn.grn_number}</td>
                          <td className="py-2 px-3">{grn.grn_date || '-'}</td>
                          <td className="py-2 px-3">{grn.delivery_ref || '-'}</td>
                          <td className="py-2 px-3">{grn.received_by || '-'}</td>
                          <td className="py-2 px-3 text-right">{formatCurrency(grn.delivery_charge)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Right Panel - PO Detail */}
        {selectedPO && activeTab === 'orders' && (
          <div className="w-[450px]">
            <div className="card">
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="font-semibold text-lg">{poDetail?.header.po_number || selectedPO}</h3>
                  <p className="text-gray-600 text-sm">{poDetail?.header.supplier_name}</p>
                </div>
                <button onClick={() => setSelectedPO(null)} className="text-gray-400 hover:text-gray-600">
                  <X className="h-5 w-5" />
                </button>
              </div>

              {detailLoading ? (
                <div className="text-center py-8 text-gray-500">Loading...</div>
              ) : poDetail ? (
                <div className="space-y-4">
                  <div className="bg-blue-50 rounded-lg p-4 text-center">
                    <div className="text-2xl font-bold text-blue-700">{formatCurrency(poDetail.header.total_value)}</div>
                    <div className="text-sm text-blue-600">Total Value</div>
                  </div>

                  {/* Action buttons */}
                  {!poDetail.header.is_cancelled && (
                    <div className="flex gap-2">
                      <button
                        onClick={openReceiveModal}
                        disabled={isSubmitting}
                        className="flex-1 flex items-center justify-center gap-2 px-3 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50"
                      >
                        <Package className="h-4 w-4" />
                        Receive Goods
                      </button>
                    </div>
                  )}

                  <div className="text-sm space-y-2">
                    <div className="flex justify-between py-1 border-b border-gray-100">
                      <span className="text-gray-500">Supplier</span>
                      <span className="font-medium">{poDetail.header.supplier_account}</span>
                    </div>
                    <div className="flex justify-between py-1 border-b border-gray-100">
                      <span className="text-gray-500">Warehouse</span>
                      <span className="font-medium">{poDetail.header.warehouse || '-'}</span>
                    </div>
                    <div className="flex justify-between py-1 border-b border-gray-100">
                      <span className="text-gray-500">Contact</span>
                      <span className="font-medium">{poDetail.header.contact || '-'}</span>
                    </div>
                    {poDetail.header.reference && (
                      <div className="flex justify-between py-1 border-b border-gray-100">
                        <span className="text-gray-500">Reference</span>
                        <span className="font-medium">{poDetail.header.reference}</span>
                      </div>
                    )}
                  </div>

                  <div>
                    <h4 className="font-medium mb-2">Lines ({poDetail.lines.length})</h4>
                    <div className="space-y-2 max-h-64 overflow-y-auto">
                      {poDetail.lines.map((line) => (
                        <div key={line.line_number} className="border border-gray-200 rounded-lg p-2 text-sm">
                          <div className="flex justify-between items-start">
                            <div>
                              <span className="font-mono text-blue-600">{line.stock_ref || line.ledger_account}</span>
                              <p className="text-gray-600 text-xs">{line.description}</p>
                            </div>
                            <span className="font-medium">{formatCurrency(line.line_value)}</span>
                          </div>
                          <div className="flex gap-4 mt-1 text-xs text-gray-500">
                            <span>Qty: {line.quantity}</span>
                            <span>@ {formatCurrency(line.unit_price)}</span>
                            {line.required_date && <span>Due: {line.required_date}</span>}
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

      {/* Create PO Modal */}
      {showCreatePOModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-3xl max-h-[90vh] overflow-y-auto">
            <div className="p-6 border-b border-gray-200">
              <div className="flex justify-between items-center">
                <h3 className="text-lg font-semibold">Create Purchase Order</h3>
                <button onClick={() => setShowCreatePOModal(false)} className="text-gray-400 hover:text-gray-600">
                  <X className="h-5 w-5" />
                </button>
              </div>
            </div>

            <div className="p-6 space-y-6">
              {actionStatus && (
                <div className={`p-4 rounded-lg ${actionStatus.type === 'success' ? 'bg-green-50 text-green-800' : 'bg-red-50 text-red-800'}`}>
                  {actionStatus.message}
                </div>
              )}

              {/* Supplier Search */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Supplier</label>
                {selectedSupplier ? (
                  <div className="flex items-center gap-2 p-3 bg-blue-50 rounded-lg">
                    <div className="flex-1">
                      <div className="font-medium">{selectedSupplier.name}</div>
                      <div className="text-sm text-gray-600">{selectedSupplier.account}</div>
                    </div>
                    <button onClick={() => { setSelectedSupplier(null); setSupplierSearch(''); }} className="text-blue-600 hover:text-blue-800">
                      Change
                    </button>
                  </div>
                ) : (
                  <div className="relative">
                    <input
                      type="text"
                      value={supplierSearch}
                      onChange={(e) => setSupplierSearch(e.target.value)}
                      placeholder="Search by account or name..."
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                    />
                    {supplierResults.length > 0 && (
                      <div className="absolute z-10 w-full mt-1 bg-white border border-gray-300 rounded-lg shadow-lg max-h-48 overflow-y-auto">
                        {supplierResults.map((s) => (
                          <button
                            key={s.account}
                            onClick={() => { setSelectedSupplier(s); setSupplierResults([]); }}
                            className="w-full px-3 py-2 text-left hover:bg-gray-50 border-b border-gray-100 last:border-b-0"
                          >
                            <div className="font-medium">{s.name}</div>
                            <div className="text-sm text-gray-500">{s.account} {s.postcode && `- ${s.postcode}`}</div>
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* PO Details */}
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Reference</label>
                  <input
                    type="text"
                    value={poReference}
                    onChange={(e) => setPOReference(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                    placeholder="Optional"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Warehouse</label>
                  <input
                    type="text"
                    value={poWarehouse}
                    onChange={(e) => setPOWarehouse(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Narrative</label>
                  <input
                    type="text"
                    value={poNarrative}
                    onChange={(e) => setPONarrative(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                    placeholder="Optional"
                  />
                </div>
              </div>

              {/* Line Items */}
              <div>
                <div className="flex justify-between items-center mb-2">
                  <label className="block text-sm font-medium text-gray-700">Line Items</label>
                  <button onClick={addLine} className="text-sm text-blue-600 hover:text-blue-800">+ Add Line</button>
                </div>
                <div className="space-y-2">
                  {poLines.map((line, index) => (
                    <div key={index} className="grid grid-cols-12 gap-2 items-start p-3 bg-gray-50 rounded-lg">
                      <div className="col-span-2">
                        <input
                          type="text"
                          value={line.stock_ref}
                          onChange={(e) => updateLine(index, 'stock_ref', e.target.value)}
                          placeholder="Stock Ref"
                          className="w-full px-2 py-1 text-sm border border-gray-300 rounded"
                        />
                      </div>
                      <div className="col-span-3">
                        <input
                          type="text"
                          value={line.description}
                          onChange={(e) => updateLine(index, 'description', e.target.value)}
                          placeholder="Description *"
                          className="w-full px-2 py-1 text-sm border border-gray-300 rounded"
                        />
                      </div>
                      <div className="col-span-2">
                        <input
                          type="number"
                          value={line.quantity}
                          onChange={(e) => updateLine(index, 'quantity', parseFloat(e.target.value) || 0)}
                          placeholder="Qty"
                          min="0"
                          step="1"
                          className="w-full px-2 py-1 text-sm border border-gray-300 rounded"
                        />
                      </div>
                      <div className="col-span-2">
                        <input
                          type="number"
                          value={line.unit_price}
                          onChange={(e) => updateLine(index, 'unit_price', parseFloat(e.target.value) || 0)}
                          placeholder="Price"
                          min="0"
                          step="0.01"
                          className="w-full px-2 py-1 text-sm border border-gray-300 rounded"
                        />
                      </div>
                      <div className="col-span-2 text-right text-sm font-medium pt-1">
                        {formatCurrency(line.quantity * line.unit_price * (1 - line.discount_percent / 100))}
                      </div>
                      <div className="col-span-1 text-right">
                        {poLines.length > 1 && (
                          <button onClick={() => removeLine(index)} className="text-red-500 hover:text-red-700 text-sm">
                            <X className="h-4 w-4" />
                          </button>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
                <div className="flex justify-end mt-2 pt-2 border-t border-gray-200">
                  <div className="text-lg font-semibold">Total: {formatCurrency(poTotal)}</div>
                </div>
              </div>
            </div>

            <div className="p-6 border-t border-gray-200 flex justify-end gap-3">
              <button onClick={() => setShowCreatePOModal(false)} className="px-4 py-2 text-gray-700 border border-gray-300 rounded-lg hover:bg-gray-50">
                Cancel
              </button>
              <button
                onClick={handleCreatePO}
                disabled={isSubmitting || !selectedSupplier}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
              >
                {isSubmitting ? 'Creating...' : 'Create Purchase Order'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Receive Goods Modal */}
      {showReceiveModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl shadow-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <div className="p-6 border-b border-gray-200">
              <div className="flex justify-between items-center">
                <h3 className="text-lg font-semibold">Receive Goods - {selectedPO}</h3>
                <button onClick={() => setShowReceiveModal(false)} className="text-gray-400 hover:text-gray-600">
                  <X className="h-5 w-5" />
                </button>
              </div>
            </div>

            <div className="p-6 space-y-6">
              {actionStatus && (
                <div className={`p-4 rounded-lg ${actionStatus.type === 'success' ? 'bg-green-50 text-green-800' : 'bg-red-50 text-red-800'}`}>
                  {actionStatus.message}
                </div>
              )}

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Delivery Reference</label>
                <input
                  type="text"
                  value={deliveryRef}
                  onChange={(e) => setDeliveryRef(e.target.value)}
                  placeholder="Carrier's delivery note number"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                />
              </div>

              {outstandingLines.length === 0 ? (
                <div className="text-center py-8 text-gray-500">No outstanding lines to receive</div>
              ) : (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Outstanding Lines</label>
                  <div className="space-y-2">
                    {outstandingLines.map((line) => (
                      <div key={line.line_number} className="flex items-center gap-4 p-3 bg-gray-50 rounded-lg">
                        <div className="flex-1">
                          <div className="font-medium">{line.stock_ref || 'Non-stock'}</div>
                          <div className="text-sm text-gray-600">{line.description}</div>
                          <div className="text-xs text-gray-500">
                            Ordered: {line.quantity_ordered} | Received: {line.quantity_received || 0} | Outstanding: {line.quantity_outstanding}
                          </div>
                        </div>
                        <div className="w-24">
                          <label className="block text-xs text-gray-500 mb-1">Receive</label>
                          <input
                            type="number"
                            value={receiveQuantities[line.line_number] || 0}
                            onChange={(e) => setReceiveQuantities({
                              ...receiveQuantities,
                              [line.line_number]: Math.min(parseFloat(e.target.value) || 0, line.quantity_outstanding || 0)
                            })}
                            max={line.quantity_outstanding}
                            min={0}
                            className="w-full px-2 py-1 border border-gray-300 rounded"
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>

            <div className="p-6 border-t border-gray-200 flex justify-end gap-3">
              <button onClick={() => setShowReceiveModal(false)} className="px-4 py-2 text-gray-700 border border-gray-300 rounded-lg hover:bg-gray-50">
                Cancel
              </button>
              <button
                onClick={handleReceiveGoods}
                disabled={isSubmitting || outstandingLines.length === 0}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
              >
                <FileCheck className="h-4 w-4" />
                {isSubmitting ? 'Creating GRN...' : 'Create GRN'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
