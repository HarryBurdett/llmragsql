import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useSearchParams, useNavigate } from 'react-router-dom';
import {
  Building,
  RefreshCw,
  Search,
  ArrowLeft,
} from 'lucide-react';
import axios from 'axios';

interface SupplierDetails {
  account: string;
  company_name: string;
  address1: string | null;
  address2: string | null;
  address3: string | null;
  address4: string | null;
  postcode: string | null;
  ac_contact: string | null;
  email: string | null;
  telephone: string | null;
  facsimile?: string | null;
  current_balance: number;
  order_balance: number;
  turnover: number;
  credit_limit?: number;
  avg_creditor_days?: number;
  first_created?: string | null;
  last_modified?: string | null;
  last_invoice: string | null;
  last_payment: string | null;
}

interface Transaction {
  date: string;
  type: string;
  ref1: string;
  ref2: string | null;
  stat: string;
  debit: number | null;
  credit: number | null;
  balance: number;
  due_date: string | null;
  unique_id: string;
  raw_type: string;
}

interface AgingAnalysis {
  '150_plus': number;
  '120_days': number;
  '90_days': number;
  '60_days': number;
  '30_days': number;
  current: number;
  total: number;
  unallocated: number;
}

interface SupplierAccountResponse {
  success: boolean;
  supplier: SupplierDetails;
  transactions: Transaction[];
  aging: AgingAnalysis;
  count: number;
  error?: string;
}

type TabType = 'general' | 'memo' | 'list';

interface SupplierSearchResult {
  account: string;
  supplier_name: string;
  balance: number;
  address1?: string;
  address2?: string;
  address3?: string;
  postcode?: string;
}

interface SearchResponse {
  success: boolean;
  suppliers: SupplierSearchResult[];
}

export function SupplierAccount() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const accountCode = searchParams.get('account') || '';
  const [searchQuery, setSearchQuery] = useState(accountCode);
  const [activeAccount, setActiveAccount] = useState(accountCode);
  const [activeTab, setActiveTab] = useState<TabType>('general');
  const [showSearch, setShowSearch] = useState(false);
  const [searchResults, setSearchResults] = useState<SupplierSearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);

  // Load first supplier by default if no account specified
  useEffect(() => {
    if (!accountCode) {
      axios.get('/api/supplier/account/first').then((response) => {
        if (response.data.success && response.data.account) {
          setActiveAccount(response.data.account);
          setSearchQuery(response.data.account);
        }
      }).catch(() => {});
    }
  }, [accountCode]);

  const accountQuery = useQuery<SupplierAccountResponse>({
    queryKey: ['supplierAccount', activeAccount],
    queryFn: async () => {
      const response = await axios.get(`/api/supplier/account/${activeAccount}`);
      return response.data;
    },
    enabled: !!activeAccount,
  });

  // Search suppliers as user types
  const handleSearchInput = async (value: string) => {
    setSearchQuery(value);
    if (value.length >= 2) {
      setIsSearching(true);
      setShowSearch(true);
      try {
        const response = await axios.get<SearchResponse>(`/api/creditors/search?query=${encodeURIComponent(value)}`);
        if (response.data.success) {
          setSearchResults(response.data.suppliers || []);
        }
      } catch {
        setSearchResults([]);
      }
      setIsSearching(false);
    } else {
      setSearchResults([]);
      setShowSearch(false);
    }
  };

  const selectSupplier = (account: string) => {
    setActiveAccount(account);
    setSearchQuery(account);
    setShowSearch(false);
    navigate(`/supplier/account?account=${account}`);
  };

  const handleSearch = () => {
    if (searchQuery.trim()) {
      setActiveAccount(searchQuery.trim().toUpperCase());
      setShowSearch(false);
      navigate(`/supplier/account?account=${searchQuery.trim().toUpperCase()}`);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
    if (e.key === 'Escape') {
      setShowSearch(false);
    }
  };

  const formatDate = (dateStr: string | null): string => {
    if (!dateStr) return '';
    try {
      const date = new Date(dateStr);
      return date.toLocaleDateString('en-GB', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
      });
    } catch {
      return dateStr;
    }
  };

  const formatCurrency = (value: number | null): string => {
    if (value === null || value === undefined) return '';
    return value.toLocaleString('en-GB', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  };

  const supplier = accountQuery.data?.supplier;
  const transactions = accountQuery.data?.transactions || [];
  const aging = accountQuery.data?.aging;

  return (
    <div className="space-y-4">
      {/* Header with Search */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <button
            onClick={() => navigate('/supplier/directory')}
            className="p-2 hover:bg-slate-100 rounded-lg transition-colors"
          >
            <ArrowLeft className="h-5 w-5 text-slate-600" />
          </button>
          <div className="p-2 bg-blue-100 rounded-lg">
            <Building className="h-6 w-6 text-blue-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-900">
              Purchase Processing
              {supplier && ` : ${activeAccount} - ${supplier.company_name}`}
            </h1>
            <p className="text-sm text-slate-500">Supplier account view</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-slate-400" />
            <input
              type="text"
              placeholder="Search supplier..."
              value={searchQuery}
              onChange={(e) => handleSearchInput(e.target.value)}
              onKeyPress={handleKeyPress}
              onFocus={() => searchResults.length > 0 && setShowSearch(true)}
              className="pl-9 pr-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 w-64"
            />
            {/* Search Results Dropdown */}
            {showSearch && (
              <div className="absolute top-full left-0 right-0 mt-1 bg-white border border-slate-200 rounded-lg shadow-lg z-50 max-h-64 overflow-y-auto">
                {isSearching ? (
                  <div className="p-3 text-center text-slate-500 text-sm">
                    <RefreshCw className="h-4 w-4 animate-spin inline mr-2" />
                    Searching...
                  </div>
                ) : searchResults.length === 0 ? (
                  <div className="p-3 text-center text-slate-500 text-sm">
                    No suppliers found
                  </div>
                ) : (
                  searchResults.map((s) => (
                    <button
                      key={s.account}
                      onClick={() => selectSupplier(s.account)}
                      className="w-full px-3 py-2 text-left hover:bg-slate-50 border-b border-slate-100 last:border-0"
                    >
                      <div className="flex justify-between items-start">
                        <div>
                          <div>
                            <span className="font-medium text-slate-900">{s.account}</span>
                            <span className="text-slate-700 ml-2">{s.supplier_name}</span>
                          </div>
                          {(s.address1 || s.address2 || s.postcode) && (
                            <div className="text-xs text-slate-500 mt-0.5">
                              {[s.address1, s.address2, s.postcode].filter(Boolean).join(', ')}
                            </div>
                          )}
                        </div>
                        <span className={`text-sm font-mono ${(s.balance || 0) > 0 ? 'text-red-600' : 'text-emerald-600'}`}>
                          {formatCurrency(s.balance)}
                        </span>
                      </div>
                    </button>
                  ))
                )}
              </div>
            )}
          </div>
          <button
            onClick={handleSearch}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            Go
          </button>
          <button
            onClick={() => accountQuery.refetch()}
            disabled={accountQuery.isFetching}
            className="flex items-center gap-2 px-4 py-2 bg-slate-100 hover:bg-slate-200 rounded-lg transition-colors"
          >
            <RefreshCw className={`h-4 w-4 ${accountQuery.isFetching ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* No Account Selected */}
      {!activeAccount && (
        <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-12 text-center">
          <Search className="h-12 w-12 mx-auto text-slate-300 mb-4" />
          <h2 className="text-lg font-semibold text-slate-700 mb-2">Enter Supplier Account</h2>
          <p className="text-slate-500">Enter a supplier account code above to view their details</p>
        </div>
      )}

      {/* Loading */}
      {activeAccount && accountQuery.isLoading && (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 text-slate-400 animate-spin" />
        </div>
      )}

      {/* Error */}
      {accountQuery.isError && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 text-red-700">
          Supplier not found or error loading data
        </div>
      )}

      {/* Main Content - Opera Style Window */}
      {supplier && (
        <div className="bg-slate-100 rounded-xl shadow-lg border border-slate-300 overflow-hidden">
          {/* Tabs */}
          <div className="bg-slate-200 border-b border-slate-300 px-2 pt-2">
            <div className="flex gap-1">
              {(['general', 'memo', 'list'] as TabType[]).map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`px-4 py-2 text-sm font-medium rounded-t-lg border border-b-0 transition-colors ${
                    activeTab === tab
                      ? 'bg-white border-slate-300 text-slate-900'
                      : 'bg-slate-100 border-transparent text-slate-600 hover:bg-slate-50'
                  }`}
                >
                  {tab.charAt(0).toUpperCase() + tab.slice(1)}
                </button>
              ))}
            </div>
          </div>

          {/* Tab Content */}
          <div className="bg-white">
            {/* General Tab */}
            {activeTab === 'general' && (
              <div className="p-6">
                <div className="grid grid-cols-2 gap-8">
                  {/* Left Column - Contact Details */}
                  <div className="space-y-4">
                    {/* Company Name */}
                    <div className="flex items-center gap-3">
                      <label className="w-32 text-sm font-medium text-slate-600">Company Name:</label>
                      <div className="flex-1 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-slate-900">
                        {supplier.company_name}
                      </div>
                    </div>

                    {/* Address */}
                    <div className="flex items-start gap-3">
                      <label className="w-32 text-sm font-medium text-slate-600 pt-2">Address:</label>
                      <div className="flex-1 space-y-1">
                        {[supplier.address1, supplier.address2, supplier.address3, supplier.address4]
                          .filter(Boolean)
                          .map((line, i) => (
                            <div
                              key={i}
                              className="px-3 py-2 bg-slate-50 border border-slate-200 rounded text-slate-900"
                            >
                              {line}
                            </div>
                          ))}
                      </div>
                    </div>

                    {/* Post Code */}
                    <div className="flex items-center gap-3">
                      <label className="w-32 text-sm font-medium text-slate-600">Post Code:</label>
                      <div className="w-32 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-slate-900">
                        {supplier.postcode || ''}
                      </div>
                    </div>

                    {/* A/C Contact */}
                    <div className="flex items-center gap-3">
                      <label className="w-32 text-sm font-medium text-slate-600">A/C Contact:</label>
                      <div className="flex-1 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-slate-900">
                        {supplier.ac_contact || ''}
                      </div>
                    </div>

                    {/* E-Mail Address */}
                    <div className="flex items-center gap-3">
                      <label className="w-32 text-sm font-medium text-slate-600">E-Mail Address:</label>
                      <div className="flex-1 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-slate-900">
                        {supplier.email || ''}
                      </div>
                    </div>

                    {/* Web Site */}
                    <div className="flex items-center gap-3">
                      <label className="w-32 text-sm font-medium text-slate-600">Web Site:</label>
                      <div className="flex-1 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-slate-900">
                        &nbsp;
                      </div>
                    </div>
                  </div>

                  {/* Right Column - Balances & Stats */}
                  <div className="space-y-4">
                    {/* Current Balance */}
                    <div className="flex items-center gap-3">
                      <label className="w-36 text-sm font-medium text-slate-600">Current Balance:</label>
                      <div className="flex items-center gap-2">
                        <button className="p-1 bg-slate-100 border border-slate-200 rounded hover:bg-slate-200">
                          <Search className="h-4 w-4 text-slate-500" />
                        </button>
                        <div className="w-28 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-right font-mono text-slate-900">
                          {formatCurrency(supplier.current_balance)}
                        </div>
                      </div>
                    </div>

                    {/* Avg Creditor Days */}
                    <div className="flex items-center gap-3">
                      <label className="w-36 text-sm font-medium text-slate-600">Avg Creditor Days:</label>
                      <div className="flex items-center gap-2">
                        <button className="p-1 bg-slate-100 border border-slate-200 rounded hover:bg-slate-200">
                          <Search className="h-4 w-4 text-slate-500" />
                        </button>
                        <div className="w-28 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-right font-mono text-slate-900">
                          {supplier.avg_creditor_days?.toFixed(1) || '0.0'}
                        </div>
                      </div>
                    </div>

                    {/* Order Balance */}
                    <div className="flex items-center gap-3">
                      <label className="w-36 text-sm font-medium text-slate-600">Order Balance:</label>
                      <div className="w-28 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-right font-mono text-slate-900 ml-8">
                        {formatCurrency(supplier.order_balance)}
                      </div>
                    </div>

                    {/* Turnover */}
                    <div className="flex items-center gap-3">
                      <label className="w-36 text-sm font-medium text-slate-600">Turnover:</label>
                      <div className="flex items-center gap-2">
                        <button className="p-1 bg-slate-100 border border-slate-200 rounded hover:bg-slate-200">
                          <Search className="h-4 w-4 text-slate-500" />
                        </button>
                        <div className="w-28 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-right font-mono text-slate-900">
                          {formatCurrency(supplier.turnover)}
                        </div>
                      </div>
                    </div>

                    {/* Credit Limit */}
                    <div className="flex items-center gap-3">
                      <label className="w-36 text-sm font-medium text-slate-600">Credit Limit:</label>
                      <div className="w-28 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-right font-mono text-slate-900 ml-8">
                        {formatCurrency(supplier.credit_limit ?? 0)}
                      </div>
                    </div>

                    {/* Telephone */}
                    <div className="flex items-center gap-3">
                      <label className="w-36 text-sm font-medium text-slate-600">Telephone:</label>
                      <div className="w-40 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-slate-900 ml-8">
                        {supplier.telephone || ''}
                      </div>
                    </div>

                    {/* Facsimile */}
                    <div className="flex items-center gap-3">
                      <label className="w-36 text-sm font-medium text-slate-600">Facsimile:</label>
                      <div className="w-40 px-3 py-2 bg-slate-50 border border-slate-200 rounded text-slate-900 ml-8">
                        {supplier.facsimile || ''}
                      </div>
                    </div>

                    {/* Dates */}
                    <div className="space-y-2 pt-2 border-t border-slate-200">
                      <div className="flex items-center gap-3">
                        <label className="w-36 text-sm font-medium text-slate-600">First Created:</label>
                        <div className="w-28 px-3 py-1.5 bg-slate-100 border border-slate-200 rounded text-slate-600 text-sm ml-8">
                          {formatDate(supplier.first_created ?? null)}
                        </div>
                      </div>
                      <div className="flex items-center gap-3">
                        <label className="w-36 text-sm font-medium text-slate-600">Last Modified:</label>
                        <div className="w-28 px-3 py-1.5 bg-slate-100 border border-slate-200 rounded text-slate-600 text-sm ml-8">
                          {formatDate(supplier.last_modified ?? null)}
                        </div>
                      </div>
                      <div className="flex items-center gap-3">
                        <label className="w-36 text-sm font-medium text-slate-600">Last Invoice:</label>
                        <div className="w-28 px-3 py-1.5 bg-slate-100 border border-slate-200 rounded text-slate-600 text-sm ml-8">
                          {formatDate(supplier.last_invoice)}
                        </div>
                      </div>
                      <div className="flex items-center gap-3">
                        <label className="w-36 text-sm font-medium text-slate-600">Last Payment:</label>
                        <div className="w-28 px-3 py-1.5 bg-slate-100 border border-slate-200 rounded text-slate-600 text-sm ml-8">
                          {formatDate(supplier.last_payment)}
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Memo Tab */}
            {activeTab === 'memo' && (
              <div className="p-6">
                <div className="bg-slate-50 border border-slate-200 rounded p-4 min-h-[300px]">
                  <p className="text-slate-400 text-sm">No memo recorded for this supplier.</p>
                </div>
              </div>
            )}

            {/* List Tab - Outstanding Transactions */}
            {activeTab === 'list' && (
              <div className="p-4">
                {/* Transactions Table */}
                <div className="border border-slate-300 rounded overflow-hidden mb-4">
                  <div className="max-h-[400px] overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-slate-100 sticky top-0">
                        <tr>
                          <th className="text-left py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Date
                          </th>
                          <th className="text-left py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Type
                          </th>
                          <th className="text-left py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Ref 1
                          </th>
                          <th className="text-left py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Ref 2
                          </th>
                          <th className="text-center py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Stat
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Debit
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Credit
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Balance
                          </th>
                          <th className="text-left py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Due Date
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {transactions.length === 0 ? (
                          <tr>
                            <td colSpan={9} className="py-8 text-center text-slate-400">
                              No outstanding transactions
                            </td>
                          </tr>
                        ) : (
                          transactions.map((txn, idx) => (
                            <tr
                              key={txn.unique_id || idx}
                              className={`${idx % 2 === 0 ? 'bg-white' : 'bg-slate-50'} hover:bg-blue-50`}
                            >
                              <td className="py-1.5 px-3 text-slate-700">{formatDate(txn.date)}</td>
                              <td className="py-1.5 px-3 text-slate-700">{txn.type}</td>
                              <td className="py-1.5 px-3 text-slate-700 font-mono text-xs">
                                {txn.ref1}
                              </td>
                              <td className="py-1.5 px-3 text-slate-600">{txn.ref2 || ''}</td>
                              <td className="py-1.5 px-3 text-center text-slate-600">{txn.stat}</td>
                              <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                                {txn.debit ? formatCurrency(txn.debit) : ''}
                              </td>
                              <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                                {txn.credit ? formatCurrency(txn.credit) : ''}
                              </td>
                              <td className="py-1.5 px-3 text-right font-mono text-slate-900 font-medium">
                                {formatCurrency(txn.balance)}
                              </td>
                              <td className="py-1.5 px-3 text-slate-700">{formatDate(txn.due_date)}</td>
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Aging Analysis */}
                {aging && (
                  <div className="border border-slate-300 rounded overflow-hidden">
                    <table className="w-full text-sm">
                      <thead className="bg-slate-100">
                        <tr>
                          <th className="text-left py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Description
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            150 Days+
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            120 Days
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            90 Days
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            60 Days
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            30 Days
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Current
                          </th>
                          <th className="text-right py-2 px-3 font-semibold text-slate-700 border-b border-slate-300">
                            Total
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        <tr className="bg-white">
                          <td className="py-1.5 px-3 text-slate-700"></td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(aging['150_plus'])}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(aging['120_days'])}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(aging['90_days'])}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(aging['60_days'])}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(aging['30_days'])}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(aging.current)}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-900 font-semibold">
                            {formatCurrency(aging.total)}
                          </td>
                        </tr>
                        <tr className="bg-slate-50">
                          <td className="py-1.5 px-3 text-slate-700">Unallocated</td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(0)}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(0)}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(0)}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(0)}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(0)}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(0)}
                          </td>
                          <td className="py-1.5 px-3 text-right font-mono text-slate-700">
                            {formatCurrency(aging.unallocated)}
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Footer Buttons */}
          <div className="bg-slate-200 border-t border-slate-300 p-3 flex justify-between items-center">
            <div className="flex items-center gap-2">
              <button className="px-4 py-1.5 bg-white border border-slate-300 rounded hover:bg-slate-50 text-sm font-medium text-slate-700">
                Action
              </button>
              <button className="p-1.5 bg-blue-500 rounded-full hover:bg-blue-600">
                <span className="text-white text-lg font-bold">?</span>
              </button>
            </div>
            <button
              onClick={() => navigate('/supplier/directory')}
              className="px-6 py-1.5 bg-white border border-slate-300 rounded hover:bg-slate-50 text-sm font-medium text-slate-700"
            >
              Close
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

export default SupplierAccount;
