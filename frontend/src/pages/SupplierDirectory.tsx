import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import {
  Building,
  RefreshCw,
  Search,
  Download,
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, LoadingState, EmptyState, Card } from '../components/ui';

interface SupplierConfig {
  account_code: string;
  name: string;
  balance: number | null;
  reconciliation_active: boolean | number;
  auto_respond: boolean | number;
  never_communicate: boolean | number;
  last_statement_date: string | null;
  payment_terms_days: number | null;
  last_synced: string | null;
}

interface SupplierConfigResponse {
  success: boolean;
  suppliers: SupplierConfig[];
  error?: string;
}

export function SupplierDirectory() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [searchQuery, setSearchQuery] = useState('');
  const [syncing, setSyncing] = useState(false);
  const [syncMessage, setSyncMessage] = useState<string | null>(null);
  const [togglingFlag, setTogglingFlag] = useState<string | null>(null);

  const configQuery = useQuery<SupplierConfigResponse>({
    queryKey: ['supplierConfig'],
    queryFn: async () => {
      const res = await authFetch('/api/supplier-config');
      if (!res.ok) throw new Error('Failed to fetch supplier config');
      return res.json();
    },
    enabled: true,
  });

  const allSuppliers: SupplierConfig[] = configQuery.data?.suppliers || [];

  const suppliers = searchQuery.trim()
    ? allSuppliers.filter(s =>
        s.name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
        s.account_code?.toLowerCase().includes(searchQuery.toLowerCase())
      )
    : allSuppliers;

  const formatCurrency = (value: number | null): string => {
    if (value === null || value === undefined) return '-';
    const isNegative = value < 0;
    return `${isNegative ? '-' : ''}£${Math.abs(value).toLocaleString('en-GB', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    })}`;
  };

  const formatDate = (date: string | null): string => {
    if (!date) return '-';
    try {
      return new Date(date).toLocaleDateString('en-GB', {
        day: '2-digit', month: 'short', year: 'numeric',
      });
    } catch {
      return date;
    }
  };

  const asBool = (val: boolean | number | null | undefined): boolean =>
    val === true || val === 1;

  const handleSyncFromOpera = async () => {
    setSyncing(true);
    setSyncMessage(null);
    try {
      const res = await authFetch('/api/supplier-config/sync', { method: 'POST' });
      const json = await res.json();
      if (!res.ok || !json.success) throw new Error(json.error || 'Sync failed');
      setSyncMessage(`Sync complete — ${json.new ?? 0} new, ${json.synced ?? 0} updated`);
      queryClient.invalidateQueries({ queryKey: ['supplierConfig'] });
    } catch (e: any) {
      setSyncMessage(`Error: ${e.message}`);
    } finally {
      setSyncing(false);
    }
  };

  const handleToggleFlag = async (
    account: string,
    flag: 'reconciliation_active' | 'auto_respond' | 'never_communicate',
    currentValue: boolean | number
  ) => {
    const key = `${account}:${flag}`;
    setTogglingFlag(key);
    try {
      const newValue = asBool(currentValue) ? 0 : 1;
      const res = await authFetch(`/api/supplier-config/${account}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ [flag]: newValue }),
      });
      const json = await res.json();
      if (!res.ok || !json.success) throw new Error(json.error || 'Update failed');
      // Optimistically update cache
      queryClient.setQueryData<SupplierConfigResponse>(['supplierConfig'], old => {
        if (!old) return old;
        return {
          ...old,
          suppliers: old.suppliers.map(s =>
            s.account_code === account ? { ...s, [flag]: newValue } : s
          ),
        };
      });
    } catch (e: any) {
      // Silently re-fetch on error to restore actual state
      queryClient.invalidateQueries({ queryKey: ['supplierConfig'] });
    } finally {
      setTogglingFlag(null);
    }
  };

  const ToggleSwitch = ({
    account,
    flag,
    value,
    label,
    activeColor = 'bg-blue-600',
  }: {
    account: string;
    flag: 'reconciliation_active' | 'auto_respond' | 'never_communicate';
    value: boolean | number;
    label: string;
    activeColor?: string;
  }) => {
    const on = asBool(value);
    const key = `${account}:${flag}`;
    const busy = togglingFlag === key;

    return (
      <div className="flex items-center gap-1.5">
        <button
          onClick={(e) => { e.stopPropagation(); handleToggleFlag(account, flag, value); }}
          disabled={busy}
          title={label}
          className={`relative inline-flex h-5 w-9 flex-shrink-0 items-center rounded-full transition-colors disabled:opacity-50 ${
            on ? activeColor : 'bg-gray-200'
          }`}
        >
          <span
            className={`inline-block h-3.5 w-3.5 transform rounded-full bg-white shadow transition-transform ${
              on ? 'translate-x-4' : 'translate-x-0.5'
            }`}
          />
        </button>
        <span className={`text-xs ${on ? 'text-gray-700' : 'text-gray-400'}`}>{label}</span>
      </div>
    );
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <PageHeader icon={Building} title="Supplier Directory" subtitle="Manage suppliers and automation settings">
        <button
          onClick={handleSyncFromOpera}
          disabled={syncing}
          className="flex items-center gap-2 px-4 py-2 bg-emerald-600 text-white rounded-lg hover:bg-emerald-700 disabled:opacity-50 transition-colors"
        >
          <Download className={`h-4 w-4 ${syncing ? 'animate-bounce' : ''}`} />
          {syncing ? 'Syncing...' : 'Sync from Opera'}
        </button>
        <button
          onClick={() => configQuery.refetch()}
          disabled={configQuery.isFetching}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors"
        >
          <RefreshCw className={`h-4 w-4 ${configQuery.isFetching ? 'animate-spin' : ''}`} />
          Refresh
        </button>
      </PageHeader>

      {/* Sync message */}
      {syncMessage && (
        <div className={`p-3 rounded-lg text-sm flex items-center justify-between ${
          syncMessage.startsWith('Error')
            ? 'bg-red-50 text-red-800 border border-red-200'
            : 'bg-emerald-50 text-emerald-800 border border-emerald-200'
        }`}>
          <span>{syncMessage}</span>
          <button onClick={() => setSyncMessage(null)} className="text-gray-500 hover:text-gray-700 ml-4">×</button>
        </div>
      )}

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400" />
        <input
          type="text"
          placeholder="Search suppliers by name or account code..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full pl-10 pr-4 py-2 border border-gray-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
        />
      </div>

      {/* Loading State */}
      {configQuery.isLoading && <LoadingState message="Loading suppliers..." />}

      {/* Error */}
      {configQuery.isError && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-sm text-red-800">
          Failed to load supplier config. Try refreshing.
        </div>
      )}

      {/* Suppliers Grid */}
      {!configQuery.isLoading && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {suppliers.length === 0 ? (
            <div className="col-span-full">
              <Card>
                <EmptyState icon={Building} title="No suppliers found" message={searchQuery ? 'Try adjusting your search' : 'Click "Sync from Opera" to import suppliers'} />
              </Card>
            </div>
          ) : (
            suppliers.map((supplier) => (
              <div
                key={supplier.account_code}
                onClick={() => navigate(`/supplier/directory/${supplier.account_code}`)}
                className="cursor-pointer"
              >
              <Card
                className="hover:border-blue-300 hover:shadow-md transition-all"
              >
                {/* Header row: name + balance */}
                <div className="flex items-start justify-between mb-1">
                  <div className="min-w-0 flex-1 mr-2">
                    <h3 className="text-sm font-semibold text-gray-900 leading-tight truncate">{supplier.name || supplier.account_code}</h3>
                    <p className="text-xs text-gray-400 mt-0.5">{supplier.account_code}</p>
                  </div>
                  <span className={`text-sm font-semibold flex-shrink-0 ${
                    supplier.balance == null ? 'text-gray-400' :
                    supplier.balance > 0 ? 'text-red-600' : 'text-emerald-600'
                  }`}>
                    {formatCurrency(supplier.balance)}
                  </span>
                </div>

                {/* Last statement date */}
                {supplier.last_statement_date && (
                  <p className="text-xs text-gray-400 mb-3">
                    Last statement: {formatDate(supplier.last_statement_date)}
                  </p>
                )}
                {!supplier.last_statement_date && (
                  <p className="text-xs text-gray-300 mb-3">No statements received</p>
                )}

                {/* Automation flag toggles */}
                <div className="border-t border-gray-100 pt-3 space-y-2">
                  <ToggleSwitch
                    account={supplier.account_code}
                    flag="reconciliation_active"
                    value={supplier.reconciliation_active}
                    label="Reconcile"
                    activeColor="bg-blue-600"
                  />
                  <ToggleSwitch
                    account={supplier.account_code}
                    flag="auto_respond"
                    value={supplier.auto_respond}
                    label="Auto-respond"
                    activeColor="bg-emerald-600"
                  />
                  <ToggleSwitch
                    account={supplier.account_code}
                    flag="never_communicate"
                    value={supplier.never_communicate}
                    label="Never communicate"
                    activeColor="bg-amber-500"
                  />
                </div>
              </Card>
              </div>
            ))
          )}
        </div>
      )}

      {/* Count */}
      {!configQuery.isLoading && suppliers.length > 0 && (
        <p className="text-xs text-gray-500 text-center">
          Showing {suppliers.length} supplier{suppliers.length !== 1 ? 's' : ''}
          {allSuppliers.length !== suppliers.length && ` (filtered from ${allSuppliers.length})`}
        </p>
      )}
    </div>
  );
}

export default SupplierDirectory;
