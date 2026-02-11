import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Truck, ChevronRight, X, Filter } from 'lucide-react';

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

async function fetchOrders(params: { status?: string; account?: string; limit?: number; offset?: number }) {
  const queryParams = new URLSearchParams();
  if (params.status) queryParams.set('status', params.status);
  if (params.account) queryParams.set('account', params.account);
  queryParams.set('limit', String(params.limit || 50));
  queryParams.set('offset', String(params.offset || 0));

  const response = await fetch(`${API_BASE}/api/pop/orders?${queryParams}`);
  if (!response.ok) throw new Error('Failed to fetch orders');
  return response.json();
}

async function fetchOrderDetail(poNumber: string): Promise<PODetail> {
  const response = await fetch(`${API_BASE}/api/pop/orders/${encodeURIComponent(poNumber)}`);
  if (!response.ok) throw new Error('Failed to fetch order detail');
  return response.json();
}

async function fetchGRNs(params: { limit?: number; offset?: number }) {
  const queryParams = new URLSearchParams();
  queryParams.set('limit', String(params.limit || 50));
  queryParams.set('offset', String(params.offset || 0));

  const response = await fetch(`${API_BASE}/api/pop/grns?${queryParams}`);
  if (!response.ok) throw new Error('Failed to fetch GRNs');
  return response.json();
}

function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  return new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(value);
}

export function PurchaseOrders() {
  const [activeTab, setActiveTab] = useState<'orders' | 'grns'>('orders');
  const [statusFilter, setStatusFilter] = useState('open');
  const [accountFilter, setAccountFilter] = useState('');
  const [selectedPO, setSelectedPO] = useState<string | null>(null);
  const [showFilters, setShowFilters] = useState(false);
  const [page, setPage] = useState(0);
  const pageSize = 50;

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

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-start">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Truck className="h-7 w-7 text-blue-600" />
            Purchase Order Processing
          </h2>
          <p className="text-gray-600 mt-1">View purchase orders and goods received notes</p>
        </div>
      </div>

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
    </div>
  );
}
