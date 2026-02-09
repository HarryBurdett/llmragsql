import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Mail,
  RefreshCw,
  Search,
  Calendar,
  Building,
  ArrowDownLeft,
  ArrowUpRight,
} from 'lucide-react';
import apiClient from '../api/client';
import type { SupplierCommunicationsResponse } from '../api/client';

export function SupplierCommunications() {
  const [searchQuery, setSearchQuery] = useState('');
  const [days, setDays] = useState(90);

  const commsQuery = useQuery<SupplierCommunicationsResponse>({
    queryKey: ['supplierCommunications', days],
    queryFn: async () => {
      const response = await apiClient.supplierCommunications(undefined, days);
      return response.data;
    },
  });

  const communications = commsQuery.data?.communications || [];

  const filteredComms = communications.filter(c => {
    if (!searchQuery) return true;
    const search = searchQuery.toLowerCase();
    return (
      c.supplier_name?.toLowerCase().includes(search) ||
      c.supplier_code?.toLowerCase().includes(search) ||
      c.email_subject?.toLowerCase().includes(search)
    );
  });

  const formatDate = (dateStr: string | null): string => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-indigo-100 rounded-lg">
            <Mail className="h-6 w-6 text-indigo-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-slate-900">Communications</h1>
            <p className="text-sm text-slate-500">Email communication history with suppliers</p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={days}
            onChange={(e) => setDays(Number(e.target.value))}
            className="px-3 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500"
          >
            <option value={30}>Last 30 days</option>
            <option value={90}>Last 90 days</option>
            <option value={180}>Last 6 months</option>
            <option value={365}>Last year</option>
          </select>
          <button
            onClick={() => commsQuery.refetch()}
            disabled={commsQuery.isFetching}
            className="flex items-center gap-2 px-4 py-2 bg-slate-100 hover:bg-slate-200 rounded-lg transition-colors"
          >
            <RefreshCw className={`h-4 w-4 ${commsQuery.isFetching ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-slate-400" />
        <input
          type="text"
          placeholder="Search by supplier or subject..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
        />
      </div>

      {/* Loading State */}
      {commsQuery.isLoading && (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 text-slate-400 animate-spin" />
        </div>
      )}

      {/* Communications List */}
      {!commsQuery.isLoading && (
        <div className="space-y-4">
          {filteredComms.length === 0 ? (
            <div className="bg-white rounded-xl shadow-sm border border-slate-200 py-12 text-center text-slate-400">
              <Mail className="h-12 w-12 mx-auto mb-3 opacity-50" />
              <p className="font-medium">No communications found</p>
              <p className="text-sm">Communication history will appear here</p>
            </div>
          ) : (
            filteredComms.map((comm) => (
              <div
                key={comm.id}
                className="bg-white rounded-xl shadow-sm border border-slate-200 p-4 hover:shadow-md transition-shadow"
              >
                <div className="flex items-start justify-between">
                  <div className="flex items-start gap-3">
                    <div className={`p-2 rounded-lg ${
                      comm.direction === 'inbound' ? 'bg-blue-100' : 'bg-emerald-100'
                    }`}>
                      {comm.direction === 'inbound' ? (
                        <ArrowDownLeft className="h-5 w-5 text-blue-600" />
                      ) : (
                        <ArrowUpRight className="h-5 w-5 text-emerald-600" />
                      )}
                    </div>
                    <div>
                      <div className="flex items-center gap-2">
                        <Building className="h-4 w-4 text-slate-400" />
                        <span className="font-medium text-slate-900">{comm.supplier_name}</span>
                        <span className="text-xs text-slate-500">({comm.supplier_code})</span>
                      </div>
                      <p className="text-sm font-medium text-slate-700 mt-1">
                        {comm.email_subject || 'No subject'}
                      </p>
                      {comm.email_body && (
                        <p className="text-sm text-slate-500 mt-1 line-clamp-2">
                          {comm.email_body.substring(0, 200)}...
                        </p>
                      )}
                    </div>
                  </div>
                  <div className="text-right">
                    <span className={`inline-flex px-2 py-1 rounded-full text-xs font-medium ${
                      comm.direction === 'inbound'
                        ? 'bg-blue-100 text-blue-700'
                        : 'bg-emerald-100 text-emerald-700'
                    }`}>
                      {comm.direction === 'inbound' ? 'Received' : 'Sent'}
                    </span>
                    <div className="flex items-center gap-1 mt-2 text-sm text-slate-500">
                      <Calendar className="h-4 w-4" />
                      {formatDate(comm.sent_at || comm.created_at)}
                    </div>
                    {comm.sent_by && (
                      <p className="text-xs text-slate-400 mt-1">By: {comm.sent_by}</p>
                    )}
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}

export default SupplierCommunications;
