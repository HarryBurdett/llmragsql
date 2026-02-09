import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Building,
  RefreshCw,
  Search,
  Mail,
  Phone,
  FileText,
  Users,
  Eye,
} from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import apiClient from '../api/client';
import type { SupplierDirectoryResponse } from '../api/client';

export function SupplierDirectory() {
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState('');

  const directoryQuery = useQuery<SupplierDirectoryResponse>({
    queryKey: ['supplierDirectory', searchQuery],
    queryFn: async () => {
      const response = await apiClient.supplierDirectory(searchQuery || undefined);
      return response.data;
    },
    enabled: true,
  });

  const suppliers = directoryQuery.data?.suppliers || [];

  const formatCurrency = (value: number | null): string => {
    if (value === null || value === undefined) return '-';
    const isNegative = value < 0;
    return `${isNegative ? '-' : ''}£${Math.abs(value).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };


  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-indigo-100 rounded-lg">
            <Building className="h-6 w-6 text-indigo-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-900">Supplier Directory</h1>
            <p className="text-sm text-slate-500">Manage suppliers and automation settings</p>
          </div>
        </div>
        <button
          onClick={() => directoryQuery.refetch()}
          disabled={directoryQuery.isFetching}
          className="flex items-center gap-2 px-4 py-2 bg-slate-100 hover:bg-slate-200 rounded-lg transition-colors"
        >
          <RefreshCw className={`h-4 w-4 ${directoryQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-slate-400" />
        <input
          type="text"
          placeholder="Search suppliers..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
        />
      </div>

      {/* Loading State */}
      {directoryQuery.isLoading && (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 text-slate-400 animate-spin" />
        </div>
      )}

      {/* Suppliers Grid */}
      {!directoryQuery.isLoading && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {suppliers.length === 0 ? (
            <div className="col-span-full bg-white rounded-xl shadow-sm border border-slate-200 py-12 text-center text-slate-400">
              <Building className="h-12 w-12 mx-auto mb-3 opacity-50" />
              <p className="font-medium">No suppliers found</p>
              <p className="text-sm">Try adjusting your search</p>
            </div>
          ) : (
            suppliers.map((supplier) => (
              <div
                key={supplier.account}
                className="bg-white rounded-xl shadow-sm border border-slate-200 p-4 hover:shadow-md transition-shadow"
              >
                <div className="flex items-start justify-between mb-3">
                  <div>
                    <h3 className="font-semibold text-slate-900">{supplier.name}</h3>
                    <p className="text-sm text-slate-500">{supplier.account}</p>
                  </div>
                  <span className={`text-sm font-semibold ${supplier.balance > 0 ? 'text-red-600' : 'text-emerald-600'}`}>
                    {formatCurrency(supplier.balance)}
                  </span>
                </div>

                <div className="space-y-2 text-sm">
                  {supplier.email && (
                    <div className="flex items-center gap-2 text-slate-600">
                      <Mail className="h-4 w-4 text-slate-400" />
                      <a href={`mailto:${supplier.email}`} className="hover:text-indigo-600">
                        {supplier.email}
                      </a>
                    </div>
                  )}
                  {supplier.phone && (
                    <div className="flex items-center gap-2 text-slate-600">
                      <Phone className="h-4 w-4 text-slate-400" />
                      {supplier.phone}
                    </div>
                  )}
                  {supplier.contact && (
                    <div className="flex items-center gap-2 text-slate-600">
                      <Users className="h-4 w-4 text-slate-400" />
                      {supplier.contact}
                    </div>
                  )}
                </div>

                <div className="mt-4 pt-3 border-t border-slate-100 flex items-center justify-between text-xs">
                  <div className="flex items-center gap-3">
                    <span className="flex items-center gap-1 text-slate-500">
                      <FileText className="h-3.5 w-3.5" />
                      {supplier.statement_count || 0} statements
                    </span>
                    <span className="flex items-center gap-1 text-slate-500">
                      <Users className="h-3.5 w-3.5" />
                      {supplier.approved_senders || 0} senders
                    </span>
                  </div>
                  <button
                    onClick={() => navigate(`/supplier/account?account=${supplier.account}`)}
                    className="flex items-center gap-1 px-2 py-1 bg-indigo-50 text-indigo-600 rounded hover:bg-indigo-100 transition-colors"
                  >
                    <Eye className="h-3.5 w-3.5" />
                    View
                  </button>
                </div>
              </div>
            ))
          )}
        </div>
      )}

      {/* Count */}
      {!directoryQuery.isLoading && suppliers.length > 0 && (
        <p className="text-sm text-slate-500 text-center">
          Showing {suppliers.length} supplier{suppliers.length !== 1 ? 's' : ''}
          {suppliers.length >= 500 && ' (limited to 500)'}
        </p>
      )}
    </div>
  );
}

export default SupplierDirectory;
