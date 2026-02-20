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
import { PageHeader, LoadingState, EmptyState, Card } from '../components/ui';

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
      <PageHeader icon={Building} title="Supplier Directory" subtitle="Manage suppliers and automation settings">
        <button
          onClick={() => directoryQuery.refetch()}
          disabled={directoryQuery.isFetching}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors"
        >
          <RefreshCw className={`h-4 w-4 ${directoryQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400" />
        <input
          type="text"
          placeholder="Search suppliers..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
        />
      </div>

      {/* Loading State */}
      {directoryQuery.isLoading && (
        <LoadingState message="Loading suppliers..." />
      )}

      {/* Suppliers Grid */}
      {!directoryQuery.isLoading && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {suppliers.length === 0 ? (
            <div className="col-span-full">
              <Card>
                <EmptyState icon={Building} title="No suppliers found" message="Try adjusting your search" />
              </Card>
            </div>
          ) : (
            suppliers.map((supplier) => (
              <Card key={supplier.account}>
                <div className="flex items-start justify-between mb-3">
                  <div>
                    <h3 className="text-base font-semibold text-gray-900">{supplier.name}</h3>
                    <p className="text-xs text-gray-500">{supplier.account}</p>
                  </div>
                  <span className={`text-sm font-semibold ${supplier.balance > 0 ? 'text-red-600' : 'text-emerald-600'}`}>
                    {formatCurrency(supplier.balance)}
                  </span>
                </div>

                <div className="space-y-2 text-sm">
                  {supplier.email && (
                    <div className="flex items-center gap-2 text-gray-600">
                      <Mail className="h-4 w-4 text-gray-400" />
                      <a href={`mailto:${supplier.email}`} className="hover:text-blue-600">
                        {supplier.email}
                      </a>
                    </div>
                  )}
                  {supplier.phone && (
                    <div className="flex items-center gap-2 text-gray-600">
                      <Phone className="h-4 w-4 text-gray-400" />
                      {supplier.phone}
                    </div>
                  )}
                  {supplier.contact && (
                    <div className="flex items-center gap-2 text-gray-600">
                      <Users className="h-4 w-4 text-gray-400" />
                      {supplier.contact}
                    </div>
                  )}
                </div>

                <div className="mt-4 pt-3 border-t border-gray-100 flex items-center justify-between text-xs">
                  <div className="flex items-center gap-3">
                    <span className="flex items-center gap-1 text-gray-500">
                      <FileText className="h-3.5 w-3.5" />
                      {supplier.statement_count || 0} statements
                    </span>
                    <span className="flex items-center gap-1 text-gray-500">
                      <Users className="h-3.5 w-3.5" />
                      {supplier.approved_senders || 0} senders
                    </span>
                  </div>
                  <button
                    onClick={() => navigate(`/supplier/account?account=${supplier.account}`)}
                    className="flex items-center gap-1 px-2 py-1 bg-blue-50 text-blue-600 rounded hover:bg-blue-100 transition-colors"
                  >
                    <Eye className="h-3.5 w-3.5" />
                    View
                  </button>
                </div>
              </Card>
            ))
          )}
        </div>
      )}

      {/* Count */}
      {!directoryQuery.isLoading && suppliers.length > 0 && (
        <p className="text-xs text-gray-500 text-center">
          Showing {suppliers.length} supplier{suppliers.length !== 1 ? 's' : ''}
          {suppliers.length >= 500 && ' (limited to 500)'}
        </p>
      )}
    </div>
  );
}

export default SupplierDirectory;
