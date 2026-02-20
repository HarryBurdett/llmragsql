import { useState } from 'react';
import { authFetch } from '../api/client';
import { useQuery } from '@tanstack/react-query';
import { Layers, ChevronRight, X, Search, Cog, Package } from 'lucide-react';
import { PageHeader, Card, LoadingState, EmptyState } from '../components/ui';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

interface Assembly {
  assembly_ref: string;
  description: string;
  category: string;
  cost_price: number;
  sell_price: number;
  component_count: number;
}

interface Component {
  component_ref: string;
  description: string;
  quantity: number;
  sequence: string;
  is_sub_assembly: boolean;
  is_phantom: boolean;
  warehouse: string;
  cost_price: number;
  in_stock: number;
  free_stock: number;
}

interface AssemblyDetail {
  assembly: {
    ref: string;
    description: string;
    category: string;
    profile: string;
    cost_price: number;
    sell_price: number;
  };
  components: Component[];
}

interface WorksOrder {
  works_order: string;
  assembly_ref: string;
  description: string;
  quantity_ordered: number;
  quantity_made: number;
  quantity_wip: number;
  quantity_allocated: number;
  order_date: string | null;
  due_date: string | null;
  completed_date: string | null;
  status: string;
  is_cancelled: boolean;
  warehouse: string;
  total_value: number;
  material_value: number;
  labour_value: number;
  sales_order_ref: string;
  job_number: string;
}

interface WOLine {
  line_number: number;
  component_ref: string;
  description: string;
  quantity_required: number;
  quantity_allocated: number;
  quantity_wip: number;
  quantity_completed: number;
  quantity_from_stock: number;
  warehouse: string;
  price: number;
  line_value: number;
  is_labour: boolean;
  is_stocked: boolean;
  is_phantom: boolean;
  issue_date: string | null;
}

interface WODetail {
  header: WorksOrder & {
    quantity_discarded: number;
    material_cost: number;
    labour_cost: number;
    assembly_cost_price: number;
    sales_account: string;
    customer_name: string;
    job_phase: string;
  };
  lines: WOLine[];
}

async function fetchAssemblies(params: { search?: string; limit?: number; offset?: number }) {
  const queryParams = new URLSearchParams();
  if (params.search) queryParams.set('search', params.search);
  queryParams.set('limit', String(params.limit || 50));
  queryParams.set('offset', String(params.offset || 0));

  const response = await authFetch(`${API_BASE}/api/bom/assemblies?${queryParams}`);
  if (!response.ok) throw new Error('Failed to fetch assemblies');
  return response.json();
}

async function fetchAssemblyDetail(ref: string): Promise<AssemblyDetail> {
  const response = await authFetch(`${API_BASE}/api/bom/assemblies/${encodeURIComponent(ref)}`);
  if (!response.ok) throw new Error('Failed to fetch assembly detail');
  return response.json();
}

async function fetchWorksOrders(params: { assembly?: string; limit?: number; offset?: number }) {
  const queryParams = new URLSearchParams();
  if (params.assembly) queryParams.set('assembly', params.assembly);
  queryParams.set('limit', String(params.limit || 50));
  queryParams.set('offset', String(params.offset || 0));

  const response = await authFetch(`${API_BASE}/api/bom/works-orders?${queryParams}`);
  if (!response.ok) throw new Error('Failed to fetch works orders');
  return response.json();
}

async function fetchWODetail(woNumber: string): Promise<WODetail> {
  const response = await authFetch(`${API_BASE}/api/bom/works-orders/${encodeURIComponent(woNumber)}`);
  if (!response.ok) throw new Error('Failed to fetch WO detail');
  return response.json();
}

function formatCurrency(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  return new Intl.NumberFormat('en-GB', { style: 'currency', currency: 'GBP' }).format(value);
}

function formatNumber(value: number | null | undefined): string {
  if (value === null || value === undefined) return '-';
  return new Intl.NumberFormat('en-GB').format(value);
}

export function BillOfMaterials() {
  const [activeTab, setActiveTab] = useState<'assemblies' | 'works-orders'>('assemblies');
  const [searchTerm, setSearchTerm] = useState('');
  const [activeSearch, setActiveSearch] = useState('');
  const [selectedAssembly, setSelectedAssembly] = useState<string | null>(null);
  const [selectedWO, setSelectedWO] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const pageSize = 50;

  const { data: assembliesData, isLoading: assembliesLoading } = useQuery({
    queryKey: ['bom-assemblies', activeSearch, page],
    queryFn: () => fetchAssemblies({ search: activeSearch, limit: pageSize, offset: page * pageSize }),
    enabled: activeTab === 'assemblies',
  });

  const { data: assemblyDetail, isLoading: assemblyDetailLoading } = useQuery({
    queryKey: ['bom-assembly-detail', selectedAssembly],
    queryFn: () => fetchAssemblyDetail(selectedAssembly!),
    enabled: !!selectedAssembly,
  });

  const { data: worksOrdersData, isLoading: woLoading } = useQuery({
    queryKey: ['bom-works-orders', page],
    queryFn: () => fetchWorksOrders({ limit: pageSize, offset: page * pageSize }),
    enabled: activeTab === 'works-orders',
  });

  const { data: woDetail, isLoading: woDetailLoading } = useQuery({
    queryKey: ['bom-wo-detail', selectedWO],
    queryFn: () => fetchWODetail(selectedWO!),
    enabled: !!selectedWO,
  });

  const assemblies: Assembly[] = assembliesData?.assemblies || [];
  const worksOrders: WorksOrder[] = worksOrdersData?.works_orders || [];
  const totalAssemblies = assembliesData?.count || 0;
  const totalWO = worksOrdersData?.count || 0;
  const totalPages = Math.ceil((activeTab === 'assemblies' ? totalAssemblies : totalWO) / pageSize);

  const handleSearch = () => {
    setActiveSearch(searchTerm);
    setPage(0);
  };

  return (
    <div className="space-y-6">
      <PageHeader icon={Layers} title="Bill of Materials" subtitle="View assembly structures and works orders" />

      <div className="flex gap-6">
        {/* Left Panel */}
        <div className="flex-1 space-y-4">
          {/* Tabs and Search */}
          <Card>
            <div className="flex gap-4 items-center">
              <div className="flex gap-2">
                <button
                  onClick={() => { setActiveTab('assemblies'); setPage(0); setSelectedWO(null); }}
                  className={`px-4 py-2 rounded-lg font-medium transition-colors flex items-center gap-2 ${
                    activeTab === 'assemblies' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  <Package className="h-4 w-4" />
                  Assemblies
                </button>
                <button
                  onClick={() => { setActiveTab('works-orders'); setPage(0); setSelectedAssembly(null); }}
                  className={`px-4 py-2 rounded-lg font-medium transition-colors flex items-center gap-2 ${
                    activeTab === 'works-orders' ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  <Cog className="h-4 w-4" />
                  Works Orders
                </button>
              </div>

              {activeTab === 'assemblies' && (
                <div className="flex-1 flex gap-2 ml-4">
                  <div className="relative flex-1">
                    <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
                    <input
                      type="text"
                      placeholder="Search assemblies..."
                      value={searchTerm}
                      onChange={(e) => setSearchTerm(e.target.value)}
                      onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
                      className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg"
                    />
                  </div>
                  <button onClick={handleSearch} className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
                    Search
                  </button>
                </div>
              )}
            </div>
          </Card>

          {/* Assemblies Table */}
          {activeTab === 'assemblies' && (
            <Card>
              <div className="flex justify-between items-center mb-4">
                <h3 className="font-semibold">
                  Assemblies {totalAssemblies > 0 && <span className="text-gray-500 font-normal">({totalAssemblies})</span>}
                </h3>
                {totalPages > 1 && (
                  <div className="flex items-center gap-2 text-sm">
                    <button onClick={() => setPage(Math.max(0, page - 1))} disabled={page === 0} className="px-2 py-1 border rounded disabled:opacity-50">Previous</button>
                    <span>Page {page + 1} of {totalPages}</span>
                    <button onClick={() => setPage(Math.min(totalPages - 1, page + 1))} disabled={page >= totalPages - 1} className="px-2 py-1 border rounded disabled:opacity-50">Next</button>
                  </div>
                )}
              </div>

              {assembliesLoading ? (
                <LoadingState message="Loading assemblies..." />
              ) : assemblies.length === 0 ? (
                <EmptyState icon={Package} title="No assemblies found" message="Try adjusting your search criteria" />
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-200">
                        <th className="text-left py-2 px-3 font-medium">Assembly</th>
                        <th className="text-left py-2 px-3 font-medium">Description</th>
                        <th className="text-center py-2 px-3 font-medium">Components</th>
                        <th className="text-right py-2 px-3 font-medium">Cost</th>
                        <th className="text-right py-2 px-3 font-medium">Sell</th>
                        <th className="w-8"></th>
                      </tr>
                    </thead>
                    <tbody>
                      {assemblies.map((asm) => (
                        <tr
                          key={asm.assembly_ref}
                          onClick={() => setSelectedAssembly(asm.assembly_ref)}
                          className={`border-b border-gray-100 cursor-pointer transition-colors ${
                            selectedAssembly === asm.assembly_ref ? 'bg-blue-50' : 'hover:bg-gray-50'
                          }`}
                        >
                          <td className="py-2 px-3 font-mono text-blue-600">{asm.assembly_ref}</td>
                          <td className="py-2 px-3">{asm.description || '-'}</td>
                          <td className="py-2 px-3 text-center">
                            <span className="px-2 py-0.5 bg-purple-100 text-purple-700 rounded text-xs">
                              {asm.component_count}
                            </span>
                          </td>
                          <td className="py-2 px-3 text-right">{formatCurrency(asm.cost_price)}</td>
                          <td className="py-2 px-3 text-right">{formatCurrency(asm.sell_price)}</td>
                          <td className="py-2 px-3"><ChevronRight className="h-4 w-4 text-gray-400" /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Card>
          )}

          {/* Works Orders Table */}
          {activeTab === 'works-orders' && (
            <Card>
              <div className="flex justify-between items-center mb-4">
                <h3 className="font-semibold">
                  Works Orders {totalWO > 0 && <span className="text-gray-500 font-normal">({totalWO})</span>}
                </h3>
              </div>

              {woLoading ? (
                <LoadingState message="Loading works orders..." />
              ) : worksOrders.length === 0 ? (
                <EmptyState icon={Cog} title="No works orders found" message="Works orders will appear here when created" />
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-200">
                        <th className="text-left py-2 px-3 font-medium">WO Number</th>
                        <th className="text-left py-2 px-3 font-medium">Assembly</th>
                        <th className="text-left py-2 px-3 font-medium">Due Date</th>
                        <th className="text-right py-2 px-3 font-medium">Ordered</th>
                        <th className="text-right py-2 px-3 font-medium">Made</th>
                        <th className="text-right py-2 px-3 font-medium">Value</th>
                        <th className="w-8"></th>
                      </tr>
                    </thead>
                    <tbody>
                      {worksOrders.map((wo) => (
                        <tr
                          key={wo.works_order}
                          onClick={() => setSelectedWO(wo.works_order)}
                          className={`border-b border-gray-100 cursor-pointer transition-colors ${
                            selectedWO === wo.works_order ? 'bg-blue-50' : 'hover:bg-gray-50'
                          }`}
                        >
                          <td className="py-2 px-3 font-mono text-blue-600">{wo.works_order}</td>
                          <td className="py-2 px-3">
                            <div className="font-medium">{wo.assembly_ref}</div>
                            <div className="text-xs text-gray-500">{wo.description}</div>
                          </td>
                          <td className="py-2 px-3">{wo.due_date || '-'}</td>
                          <td className="py-2 px-3 text-right">{formatNumber(wo.quantity_ordered)}</td>
                          <td className="py-2 px-3 text-right">
                            <span className={wo.quantity_made >= wo.quantity_ordered ? 'text-green-600' : ''}>
                              {formatNumber(wo.quantity_made)}
                            </span>
                          </td>
                          <td className="py-2 px-3 text-right font-medium">{formatCurrency(wo.total_value)}</td>
                          <td className="py-2 px-3"><ChevronRight className="h-4 w-4 text-gray-400" /></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </Card>
          )}
        </div>

        {/* Right Panel - Assembly Detail */}
        {selectedAssembly && activeTab === 'assemblies' && (
          <div className="w-[450px]">
            <Card>
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="font-semibold text-lg">{assemblyDetail?.assembly.ref || selectedAssembly}</h3>
                  <p className="text-gray-600 text-sm">{assemblyDetail?.assembly.description}</p>
                </div>
                <button onClick={() => setSelectedAssembly(null)} className="text-gray-400 hover:text-gray-600">
                  <X className="h-5 w-5" />
                </button>
              </div>

              {assemblyDetailLoading ? (
                <LoadingState message="Loading assembly details..." />
              ) : assemblyDetail ? (
                <div className="space-y-4">
                  <div className="grid grid-cols-2 gap-3">
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <div className="text-lg font-bold">{formatCurrency(assemblyDetail.assembly.cost_price)}</div>
                      <div className="text-xs text-gray-500">Cost Price</div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3 text-center">
                      <div className="text-lg font-bold">{formatCurrency(assemblyDetail.assembly.sell_price)}</div>
                      <div className="text-xs text-gray-500">Sell Price</div>
                    </div>
                  </div>

                  <div>
                    <h4 className="font-medium mb-2">Components ({assemblyDetail.components.length})</h4>
                    <div className="space-y-2 max-h-96 overflow-y-auto">
                      {assemblyDetail.components.map((comp, idx) => (
                        <div key={idx} className="border border-gray-200 rounded-lg p-2 text-sm">
                          <div className="flex justify-between items-start">
                            <div>
                              <span className="font-mono text-blue-600">{comp.component_ref}</span>
                              {comp.is_sub_assembly && (
                                <span className="ml-2 px-1.5 py-0.5 bg-purple-100 text-purple-700 rounded text-xs">Sub-Asm</span>
                              )}
                              <p className="text-gray-600 text-xs">{comp.description}</p>
                            </div>
                            <span className="font-medium">x{formatNumber(comp.quantity)}</span>
                          </div>
                          <div className="flex gap-4 mt-1 text-xs text-gray-500">
                            <span>Stock: {formatNumber(comp.in_stock)}</span>
                            <span>Free: {formatNumber(comp.free_stock)}</span>
                            <span>Cost: {formatCurrency(comp.cost_price)}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              ) : null}
            </Card>
          </div>
        )}

        {/* Right Panel - Works Order Detail */}
        {selectedWO && activeTab === 'works-orders' && (
          <div className="w-[450px]">
            <Card>
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="font-semibold text-lg">{woDetail?.header.works_order || selectedWO}</h3>
                  <p className="text-gray-600 text-sm">{woDetail?.header.description}</p>
                </div>
                <button onClick={() => setSelectedWO(null)} className="text-gray-400 hover:text-gray-600">
                  <X className="h-5 w-5" />
                </button>
              </div>

              {woDetailLoading ? (
                <LoadingState message="Loading works order details..." />
              ) : woDetail ? (
                <div className="space-y-4">
                  {/* Progress */}
                  <div className="grid grid-cols-4 gap-2">
                    <div className="bg-blue-50 rounded-lg p-2 text-center">
                      <div className="text-lg font-bold text-blue-700">{formatNumber(woDetail.header.quantity_ordered)}</div>
                      <div className="text-xs text-blue-600">Ordered</div>
                    </div>
                    <div className="bg-yellow-50 rounded-lg p-2 text-center">
                      <div className="text-lg font-bold text-yellow-700">{formatNumber(woDetail.header.quantity_wip)}</div>
                      <div className="text-xs text-yellow-600">WIP</div>
                    </div>
                    <div className="bg-green-50 rounded-lg p-2 text-center">
                      <div className="text-lg font-bold text-green-700">{formatNumber(woDetail.header.quantity_made)}</div>
                      <div className="text-xs text-green-600">Made</div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-2 text-center">
                      <div className="text-lg font-bold">{formatCurrency(woDetail.header.total_value)}</div>
                      <div className="text-xs text-gray-500">Value</div>
                    </div>
                  </div>

                  <div className="text-sm space-y-2">
                    <div className="flex justify-between py-1 border-b border-gray-100">
                      <span className="text-gray-500">Assembly</span>
                      <span className="font-medium">{woDetail.header.assembly_ref}</span>
                    </div>
                    <div className="flex justify-between py-1 border-b border-gray-100">
                      <span className="text-gray-500">Order Date</span>
                      <span className="font-medium">{woDetail.header.order_date || '-'}</span>
                    </div>
                    <div className="flex justify-between py-1 border-b border-gray-100">
                      <span className="text-gray-500">Due Date</span>
                      <span className="font-medium">{woDetail.header.due_date || '-'}</span>
                    </div>
                    {woDetail.header.sales_order_ref && (
                      <div className="flex justify-between py-1 border-b border-gray-100">
                        <span className="text-gray-500">Sales Order</span>
                        <span className="font-medium">{woDetail.header.sales_order_ref}</span>
                      </div>
                    )}
                  </div>

                  <div>
                    <h4 className="font-medium mb-2">Components ({woDetail.lines.length})</h4>
                    <div className="space-y-2 max-h-64 overflow-y-auto">
                      {woDetail.lines.map((line) => (
                        <div key={line.line_number} className="border border-gray-200 rounded-lg p-2 text-sm">
                          <div className="flex justify-between items-start">
                            <div>
                              <span className="font-mono text-blue-600">{line.component_ref}</span>
                              {line.is_labour && <span className="ml-2 px-1.5 py-0.5 bg-orange-100 text-orange-700 rounded text-xs">Labour</span>}
                              <p className="text-gray-600 text-xs">{line.description}</p>
                            </div>
                          </div>
                          <div className="flex gap-4 mt-1 text-xs text-gray-500">
                            <span>Req: {formatNumber(line.quantity_required)}</span>
                            <span>Alloc: {formatNumber(line.quantity_allocated)}</span>
                            <span>Done: {formatNumber(line.quantity_completed)}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              ) : null}
            </Card>
          </div>
        )}
      </div>
    </div>
  );
}
