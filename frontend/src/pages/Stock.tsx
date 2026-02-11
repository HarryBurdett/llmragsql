import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Package, Search, Warehouse, Filter, ChevronRight, ChevronDown, X, History, Tag, Layers } from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000';

interface Product {
  ref: string;
  description: string;
  category: string;
  profile: string;
  cost_price: number;
  sell_price: number;
  total_in_stock: number;
  free_stock: number;
  allocated: number;
  on_order: number;
  on_sales_order: number;
  alt_code_1: string;
  alt_code_2: string;
  analysis_code: string;
  last_issued: string | null;
  last_received: string | null;
}

interface ProductDetail {
  product: {
    ref: string;
    description: string;
    category_code: string;
    category_name: string;
    profile_code: string;
    profile_name: string;
    cost_price: number;
    last_cost_price: number;
    sell_price: number;
    sale_price: number;
    total_in_stock: number;
    free_stock: number;
    allocated: number;
    on_order: number;
    on_sales_order: number;
    alt_code_1: string;
    alt_code_2: string;
    alt_code_3: string;
    sales_analysis_code: string;
    purchase_analysis_code: string;
    is_stocked: boolean;
    is_batch_tracked: boolean;
    is_serial_tracked: boolean;
    is_fifo: boolean;
    is_average_costed: boolean;
    last_issued: string | null;
    last_received: string | null;
  };
  stock_by_warehouse: WarehouseStock[];
}

interface WarehouseStock {
  warehouse_code: string;
  warehouse_name: string;
  in_stock: number;
  free_stock: number;
  allocated: number;
  on_order: number;
  on_sales_order: number;
  cost_price: number;
  sell_price: number;
  bin_location: string;
  reorder_level: number;
  reorder_quantity: number;
  last_issued: string | null;
  last_received: string | null;
}

interface Transaction {
  stock_ref: string;
  warehouse: string;
  warehouse_name: string;
  trans_type: string;
  trans_type_desc: string;
  trans_date: string;
  quantity: number;
  reference: string;
  account: string;
  cost_value: number;
  sell_value: number;
  comment: string;
}

interface Category {
  code: string;
  description: string;
}

interface Profile {
  code: string;
  name: string;
  is_stocked: boolean;
  is_batch_tracked: boolean;
  is_serial_tracked: boolean;
}

interface Warehouse {
  code: string;
  name: string;
}

async function fetchProducts(params: {
  search?: string;
  category?: string;
  profile?: string;
  limit?: number;
  offset?: number;
}) {
  const queryParams = new URLSearchParams();
  if (params.search) queryParams.set('search', params.search);
  if (params.category) queryParams.set('category', params.category);
  if (params.profile) queryParams.set('profile', params.profile);
  queryParams.set('limit', String(params.limit || 50));
  queryParams.set('offset', String(params.offset || 0));

  const response = await fetch(`${API_BASE}/api/stock/products?${queryParams}`);
  if (!response.ok) throw new Error('Failed to fetch products');
  return response.json();
}

async function fetchProductDetail(ref: string): Promise<ProductDetail> {
  const response = await fetch(`${API_BASE}/api/stock/products/${encodeURIComponent(ref)}`);
  if (!response.ok) throw new Error('Failed to fetch product detail');
  return response.json();
}

async function fetchTransactions(ref: string, limit: number = 50): Promise<{ transactions: Transaction[]; count: number }> {
  const response = await fetch(`${API_BASE}/api/stock/products/${encodeURIComponent(ref)}/transactions?limit=${limit}`);
  if (!response.ok) throw new Error('Failed to fetch transactions');
  return response.json();
}

async function fetchCategories(): Promise<{ categories: Category[] }> {
  const response = await fetch(`${API_BASE}/api/stock/categories`);
  if (!response.ok) throw new Error('Failed to fetch categories');
  return response.json();
}

async function fetchProfiles(): Promise<{ profiles: Profile[] }> {
  const response = await fetch(`${API_BASE}/api/stock/profiles`);
  if (!response.ok) throw new Error('Failed to fetch profiles');
  return response.json();
}

async function fetchWarehouses(): Promise<{ warehouses: Warehouse[] }> {
  const response = await fetch(`${API_BASE}/api/stock/warehouses`);
  if (!response.ok) throw new Error('Failed to fetch warehouses');
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

export function Stock() {
  const [searchTerm, setSearchTerm] = useState('');
  const [activeSearch, setActiveSearch] = useState('');
  const [selectedCategory, setSelectedCategory] = useState('');
  const [selectedProfile, setSelectedProfile] = useState('');
  const [selectedProduct, setSelectedProduct] = useState<string | null>(null);
  const [showFilters, setShowFilters] = useState(false);
  const [activeTab, setActiveTab] = useState<'details' | 'stock' | 'transactions'>('details');
  const [page, setPage] = useState(0);
  const pageSize = 50;

  // Fetch reference data
  const { data: categoriesData } = useQuery({
    queryKey: ['stock-categories'],
    queryFn: fetchCategories,
  });

  const { data: profilesData } = useQuery({
    queryKey: ['stock-profiles'],
    queryFn: fetchProfiles,
  });

  const { data: warehousesData } = useQuery({
    queryKey: ['stock-warehouses'],
    queryFn: fetchWarehouses,
  });

  // Fetch products
  const { data: productsData, isLoading: productsLoading } = useQuery({
    queryKey: ['stock-products', activeSearch, selectedCategory, selectedProfile, page],
    queryFn: () => fetchProducts({
      search: activeSearch,
      category: selectedCategory,
      profile: selectedProfile,
      limit: pageSize,
      offset: page * pageSize,
    }),
  });

  // Fetch product detail when selected
  const { data: productDetail, isLoading: detailLoading } = useQuery({
    queryKey: ['stock-product-detail', selectedProduct],
    queryFn: () => fetchProductDetail(selectedProduct!),
    enabled: !!selectedProduct,
  });

  // Fetch transactions when on transactions tab
  const { data: transactionsData, isLoading: transactionsLoading } = useQuery({
    queryKey: ['stock-transactions', selectedProduct],
    queryFn: () => fetchTransactions(selectedProduct!),
    enabled: !!selectedProduct && activeTab === 'transactions',
  });

  const handleSearch = () => {
    setActiveSearch(searchTerm);
    setPage(0);
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const clearFilters = () => {
    setSearchTerm('');
    setActiveSearch('');
    setSelectedCategory('');
    setSelectedProfile('');
    setPage(0);
  };

  const products: Product[] = productsData?.products || [];
  const totalProducts = productsData?.count || 0;
  const categories = categoriesData?.categories || [];
  const profiles = profilesData?.profiles || [];
  const warehouses = warehousesData?.warehouses || [];
  const totalPages = Math.ceil(totalProducts / pageSize);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h2 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
            <Package className="h-7 w-7 text-blue-600" />
            Stock
          </h2>
          <p className="text-gray-600 mt-1">Browse and search stock products</p>
        </div>
        <div className="text-sm text-gray-500">
          {warehouses.length} warehouses | {categories.length} categories | {profiles.length} profiles
        </div>
      </div>

      <div className="flex gap-6">
        {/* Left Panel - Product List */}
        <div className="flex-1 space-y-4">
          {/* Search and Filters */}
          <div className="card">
            <div className="flex gap-4 items-center">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search by reference, description, or alt codes..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  onKeyPress={handleKeyPress}
                  className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              <button
                onClick={handleSearch}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
              >
                Search
              </button>
              <button
                onClick={() => setShowFilters(!showFilters)}
                className={`px-4 py-2 border rounded-lg transition-colors flex items-center gap-2 ${
                  showFilters || selectedCategory || selectedProfile
                    ? 'bg-blue-50 border-blue-300 text-blue-700'
                    : 'border-gray-300 text-gray-700 hover:bg-gray-50'
                }`}
              >
                <Filter className="h-4 w-4" />
                Filters
                {(selectedCategory || selectedProfile) && (
                  <span className="bg-blue-600 text-white text-xs px-1.5 py-0.5 rounded-full">
                    {(selectedCategory ? 1 : 0) + (selectedProfile ? 1 : 0)}
                  </span>
                )}
              </button>
            </div>

            {/* Filter Panel */}
            {showFilters && (
              <div className="mt-4 pt-4 border-t border-gray-200 grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Category</label>
                  <select
                    value={selectedCategory}
                    onChange={(e) => { setSelectedCategory(e.target.value); setPage(0); }}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="">All Categories</option>
                    {categories.map((cat) => (
                      <option key={cat.code} value={cat.code}>
                        {cat.code} - {cat.description}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Profile</label>
                  <select
                    value={selectedProfile}
                    onChange={(e) => { setSelectedProfile(e.target.value); setPage(0); }}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="">All Profiles</option>
                    {profiles.map((prof) => (
                      <option key={prof.code} value={prof.code}>
                        {prof.code} - {prof.name}
                      </option>
                    ))}
                  </select>
                </div>
                {(activeSearch || selectedCategory || selectedProfile) && (
                  <div className="col-span-2">
                    <button
                      onClick={clearFilters}
                      className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1"
                    >
                      <X className="h-4 w-4" />
                      Clear all filters
                    </button>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Products List */}
          <div className="card">
            <div className="flex justify-between items-center mb-4">
              <h3 className="font-semibold text-gray-900">
                Products {totalProducts > 0 && <span className="text-gray-500 font-normal">({totalProducts})</span>}
              </h3>
              {totalPages > 1 && (
                <div className="flex items-center gap-2 text-sm">
                  <button
                    onClick={() => setPage(Math.max(0, page - 1))}
                    disabled={page === 0}
                    className="px-2 py-1 border rounded disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
                  >
                    Previous
                  </button>
                  <span className="text-gray-600">
                    Page {page + 1} of {totalPages}
                  </span>
                  <button
                    onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
                    disabled={page >= totalPages - 1}
                    className="px-2 py-1 border rounded disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-50"
                  >
                    Next
                  </button>
                </div>
              )}
            </div>

            {productsLoading ? (
              <div className="text-center py-8 text-gray-500">Loading products...</div>
            ) : products.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                No products found. Try adjusting your search or filters.
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-200">
                      <th className="text-left py-2 px-3 font-medium text-gray-700">Reference</th>
                      <th className="text-left py-2 px-3 font-medium text-gray-700">Description</th>
                      <th className="text-left py-2 px-3 font-medium text-gray-700">Category</th>
                      <th className="text-right py-2 px-3 font-medium text-gray-700">In Stock</th>
                      <th className="text-right py-2 px-3 font-medium text-gray-700">Free</th>
                      <th className="text-right py-2 px-3 font-medium text-gray-700">Cost</th>
                      <th className="text-right py-2 px-3 font-medium text-gray-700">Sell</th>
                      <th className="w-8"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {products.map((product) => (
                      <tr
                        key={product.ref}
                        onClick={() => setSelectedProduct(product.ref)}
                        className={`border-b border-gray-100 cursor-pointer transition-colors ${
                          selectedProduct === product.ref
                            ? 'bg-blue-50'
                            : 'hover:bg-gray-50'
                        }`}
                      >
                        <td className="py-2 px-3 font-mono text-blue-600">{product.ref}</td>
                        <td className="py-2 px-3 truncate max-w-xs" title={product.description}>
                          {product.description}
                        </td>
                        <td className="py-2 px-3">
                          <span className="px-2 py-0.5 bg-gray-100 rounded text-xs">
                            {product.category}
                          </span>
                        </td>
                        <td className="py-2 px-3 text-right">{formatNumber(product.total_in_stock)}</td>
                        <td className="py-2 px-3 text-right">{formatNumber(product.free_stock)}</td>
                        <td className="py-2 px-3 text-right">{formatCurrency(product.cost_price)}</td>
                        <td className="py-2 px-3 text-right">{formatCurrency(product.sell_price)}</td>
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

        {/* Right Panel - Product Detail */}
        {selectedProduct && (
          <div className="w-[450px] space-y-4">
            <div className="card">
              <div className="flex justify-between items-start mb-4">
                <div>
                  <h3 className="font-semibold text-lg text-gray-900">
                    {productDetail?.product.ref || selectedProduct}
                  </h3>
                  <p className="text-gray-600 text-sm">
                    {productDetail?.product.description || 'Loading...'}
                  </p>
                </div>
                <button
                  onClick={() => setSelectedProduct(null)}
                  className="text-gray-400 hover:text-gray-600"
                >
                  <X className="h-5 w-5" />
                </button>
              </div>

              {/* Tabs */}
              <div className="flex border-b border-gray-200 mb-4">
                <button
                  onClick={() => setActiveTab('details')}
                  className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                    activeTab === 'details'
                      ? 'border-blue-600 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700'
                  }`}
                >
                  <Tag className="h-4 w-4 inline mr-1" />
                  Details
                </button>
                <button
                  onClick={() => setActiveTab('stock')}
                  className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                    activeTab === 'stock'
                      ? 'border-blue-600 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700'
                  }`}
                >
                  <Warehouse className="h-4 w-4 inline mr-1" />
                  Stock ({productDetail?.stock_by_warehouse.length || 0})
                </button>
                <button
                  onClick={() => setActiveTab('transactions')}
                  className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                    activeTab === 'transactions'
                      ? 'border-blue-600 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700'
                  }`}
                >
                  <History className="h-4 w-4 inline mr-1" />
                  History
                </button>
              </div>

              {detailLoading ? (
                <div className="text-center py-8 text-gray-500">Loading...</div>
              ) : productDetail ? (
                <>
                  {/* Details Tab */}
                  {activeTab === 'details' && (
                    <div className="space-y-4">
                      {/* Summary Stats */}
                      <div className="grid grid-cols-3 gap-3">
                        <div className="bg-blue-50 rounded-lg p-3 text-center">
                          <div className="text-2xl font-bold text-blue-700">
                            {formatNumber(productDetail.product.total_in_stock)}
                          </div>
                          <div className="text-xs text-blue-600">In Stock</div>
                        </div>
                        <div className="bg-green-50 rounded-lg p-3 text-center">
                          <div className="text-2xl font-bold text-green-700">
                            {formatNumber(productDetail.product.free_stock)}
                          </div>
                          <div className="text-xs text-green-600">Free Stock</div>
                        </div>
                        <div className="bg-orange-50 rounded-lg p-3 text-center">
                          <div className="text-2xl font-bold text-orange-700">
                            {formatNumber(productDetail.product.allocated)}
                          </div>
                          <div className="text-xs text-orange-600">Allocated</div>
                        </div>
                      </div>

                      {/* Product Info */}
                      <div className="space-y-3 text-sm">
                        <div className="flex justify-between py-2 border-b border-gray-100">
                          <span className="text-gray-500">Category</span>
                          <span className="font-medium">
                            {productDetail.product.category_code} - {productDetail.product.category_name}
                          </span>
                        </div>
                        <div className="flex justify-between py-2 border-b border-gray-100">
                          <span className="text-gray-500">Profile</span>
                          <span className="font-medium">
                            {productDetail.product.profile_code} - {productDetail.product.profile_name}
                          </span>
                        </div>
                        <div className="flex justify-between py-2 border-b border-gray-100">
                          <span className="text-gray-500">Cost Price</span>
                          <span className="font-medium">{formatCurrency(productDetail.product.cost_price)}</span>
                        </div>
                        <div className="flex justify-between py-2 border-b border-gray-100">
                          <span className="text-gray-500">Last Cost</span>
                          <span className="font-medium">{formatCurrency(productDetail.product.last_cost_price)}</span>
                        </div>
                        <div className="flex justify-between py-2 border-b border-gray-100">
                          <span className="text-gray-500">Sell Price</span>
                          <span className="font-medium">{formatCurrency(productDetail.product.sell_price)}</span>
                        </div>
                        <div className="flex justify-between py-2 border-b border-gray-100">
                          <span className="text-gray-500">On Order (PO)</span>
                          <span className="font-medium">{formatNumber(productDetail.product.on_order)}</span>
                        </div>
                        <div className="flex justify-between py-2 border-b border-gray-100">
                          <span className="text-gray-500">On Sales Order</span>
                          <span className="font-medium">{formatNumber(productDetail.product.on_sales_order)}</span>
                        </div>
                        <div className="flex justify-between py-2 border-b border-gray-100">
                          <span className="text-gray-500">Last Issued</span>
                          <span className="font-medium">{productDetail.product.last_issued || '-'}</span>
                        </div>
                        <div className="flex justify-between py-2 border-b border-gray-100">
                          <span className="text-gray-500">Last Received</span>
                          <span className="font-medium">{productDetail.product.last_received || '-'}</span>
                        </div>
                      </div>

                      {/* Flags */}
                      <div className="flex flex-wrap gap-2 pt-2">
                        {productDetail.product.is_stocked && (
                          <span className="px-2 py-1 bg-green-100 text-green-700 text-xs rounded">Stocked</span>
                        )}
                        {!productDetail.product.is_stocked && (
                          <span className="px-2 py-1 bg-gray-100 text-gray-700 text-xs rounded">Non-Stocked</span>
                        )}
                        {productDetail.product.is_batch_tracked && (
                          <span className="px-2 py-1 bg-purple-100 text-purple-700 text-xs rounded">Batch Tracked</span>
                        )}
                        {productDetail.product.is_serial_tracked && (
                          <span className="px-2 py-1 bg-purple-100 text-purple-700 text-xs rounded">Serial Tracked</span>
                        )}
                        {productDetail.product.is_fifo && (
                          <span className="px-2 py-1 bg-blue-100 text-blue-700 text-xs rounded">FIFO</span>
                        )}
                        {productDetail.product.is_average_costed && (
                          <span className="px-2 py-1 bg-blue-100 text-blue-700 text-xs rounded">Average Costed</span>
                        )}
                      </div>
                    </div>
                  )}

                  {/* Stock Tab */}
                  {activeTab === 'stock' && (
                    <div>
                      {productDetail.stock_by_warehouse.length === 0 ? (
                        <div className="text-center py-8 text-gray-500">
                          No warehouse stock records for this product.
                        </div>
                      ) : (
                        <div className="space-y-3">
                          {productDetail.stock_by_warehouse.map((wh) => (
                            <div
                              key={wh.warehouse_code}
                              className="border border-gray-200 rounded-lg p-3"
                            >
                              <div className="flex justify-between items-center mb-2">
                                <div className="flex items-center gap-2">
                                  <Warehouse className="h-4 w-4 text-gray-400" />
                                  <span className="font-medium">{wh.warehouse_code}</span>
                                  <span className="text-gray-500 text-sm">{wh.warehouse_name}</span>
                                </div>
                                {wh.bin_location && (
                                  <span className="text-xs bg-gray-100 px-2 py-1 rounded">
                                    Bin: {wh.bin_location}
                                  </span>
                                )}
                              </div>
                              <div className="grid grid-cols-4 gap-2 text-sm">
                                <div>
                                  <div className="text-gray-500 text-xs">In Stock</div>
                                  <div className="font-medium">{formatNumber(wh.in_stock)}</div>
                                </div>
                                <div>
                                  <div className="text-gray-500 text-xs">Free</div>
                                  <div className="font-medium text-green-600">{formatNumber(wh.free_stock)}</div>
                                </div>
                                <div>
                                  <div className="text-gray-500 text-xs">Allocated</div>
                                  <div className="font-medium text-orange-600">{formatNumber(wh.allocated)}</div>
                                </div>
                                <div>
                                  <div className="text-gray-500 text-xs">On Order</div>
                                  <div className="font-medium">{formatNumber(wh.on_order)}</div>
                                </div>
                              </div>
                              {(wh.reorder_level > 0 || wh.reorder_quantity > 0) && (
                                <div className="mt-2 pt-2 border-t border-gray-100 text-xs text-gray-500">
                                  Reorder: Level {formatNumber(wh.reorder_level)}, Qty {formatNumber(wh.reorder_quantity)}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}

                  {/* Transactions Tab */}
                  {activeTab === 'transactions' && (
                    <div>
                      {transactionsLoading ? (
                        <div className="text-center py-8 text-gray-500">Loading transactions...</div>
                      ) : !transactionsData?.transactions.length ? (
                        <div className="text-center py-8 text-gray-500">
                          No transaction history for this product.
                        </div>
                      ) : (
                        <div className="space-y-2 max-h-96 overflow-y-auto">
                          {transactionsData.transactions.map((trans, idx) => (
                            <div
                              key={idx}
                              className="border border-gray-200 rounded-lg p-3 text-sm"
                            >
                              <div className="flex justify-between items-start mb-1">
                                <div className="flex items-center gap-2">
                                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                    trans.trans_type === 'R' || trans.trans_type === 'M'
                                      ? 'bg-green-100 text-green-700'
                                      : trans.trans_type === 'I' || trans.trans_type === 'S' || trans.trans_type === 'W'
                                      ? 'bg-red-100 text-red-700'
                                      : 'bg-gray-100 text-gray-700'
                                  }`}>
                                    {trans.trans_type_desc}
                                  </span>
                                  <span className="text-gray-500">{trans.warehouse}</span>
                                </div>
                                <span className="text-gray-500">{trans.trans_date}</span>
                              </div>
                              <div className="flex justify-between items-center">
                                <span className="text-gray-600">{trans.reference || trans.comment || '-'}</span>
                                <span className={`font-medium ${
                                  trans.quantity > 0 ? 'text-green-600' : 'text-red-600'
                                }`}>
                                  {trans.quantity > 0 ? '+' : ''}{formatNumber(trans.quantity)}
                                </span>
                              </div>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}
                </>
              ) : null}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
