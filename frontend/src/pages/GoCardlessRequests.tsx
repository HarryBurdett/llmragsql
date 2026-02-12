import { useState, useEffect, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  CreditCard, CheckCircle, AlertCircle, Clock, RefreshCw, Plus,
  Send, X, Link, Unlink, FileText, Users, Ban, History, Search
} from 'lucide-react';
import { authFetch } from '../api/client';

// Searchable customer dropdown component
function CustomerAccountSearch({
  value,
  valueName,
  onChange,
  placeholder = "Type to search customers..."
}: {
  value: string;
  valueName?: string;
  onChange: (account: string, name: string) => void;
  placeholder?: string;
}) {
  const [search, setSearch] = useState('');
  const [isOpen, setIsOpen] = useState(false);
  const [results, setResults] = useState<Array<{account: string; name: string; postcode?: string}>>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(0);
  const [selectedName, setSelectedName] = useState(valueName || '');
  const wrapperRef = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLDivElement>(null);

  // Sync selectedName when valueName prop changes
  useEffect(() => {
    if (valueName) setSelectedName(valueName);
  }, [valueName]);

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (wrapperRef.current && !wrapperRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Debounced search
  useEffect(() => {
    if (search.length < 2) {
      setResults([]);
      return;
    }
    const timer = setTimeout(async () => {
      setIsLoading(true);
      try {
        const res = await authFetch(`/api/sop/customers?search=${encodeURIComponent(search)}&limit=20`);
        const data = await res.json();
        setResults(data.customers || []);
        setHighlightedIndex(0);
      } catch {
        setResults([]);
      }
      setIsLoading(false);
    }, 300);
    return () => clearTimeout(timer);
  }, [search]);

  // Scroll highlighted item into view
  useEffect(() => {
    if (listRef.current && isOpen) {
      const highlighted = listRef.current.children[highlightedIndex] as HTMLElement;
      if (highlighted) {
        highlighted.scrollIntoView({ block: 'nearest' });
      }
    }
  }, [highlightedIndex, isOpen]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!isOpen && results.length === 0) return;

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setIsOpen(true);
        setHighlightedIndex(i => Math.min(i + 1, results.length - 1));
        break;
      case 'ArrowUp':
        e.preventDefault();
        setHighlightedIndex(i => Math.max(i - 1, 0));
        break;
      case 'Enter':
        e.preventDefault();
        if (results[highlightedIndex]) {
          const c = results[highlightedIndex];
          onChange(c.account, c.name);
          setSelectedName(c.name);
          setIsOpen(false);
          setSearch('');
        }
        break;
      case 'Escape':
        setIsOpen(false);
        break;
    }
  };

  const handleSelect = (c: {account: string; name: string}) => {
    onChange(c.account, c.name);
    setSelectedName(c.name);
    setIsOpen(false);
    setSearch('');
  };

  return (
    <div ref={wrapperRef} className="relative">
      {value ? (
        <div className="flex items-center gap-2 p-2 border border-green-300 bg-green-50 rounded text-sm">
          <span className="flex-1 truncate font-medium">{value}</span>
          <span className="text-gray-500 truncate">{selectedName || valueName}</span>
          <button
            type="button"
            onClick={() => { onChange('', ''); setSelectedName(''); setSearch(''); }}
            className="text-gray-400 hover:text-red-500"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      ) : (
        <div className="relative">
          <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            className="w-full pl-9 pr-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-green-500 focus:border-green-500"
            placeholder={placeholder}
            value={search}
            onChange={(e) => { setSearch(e.target.value); setIsOpen(true); }}
            onFocus={() => setIsOpen(true)}
            onKeyDown={handleKeyDown}
          />
          {isLoading && (
            <RefreshCw className="w-4 h-4 absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-400 animate-spin" />
          )}
        </div>
      )}
      {isOpen && !value && search.length >= 2 && (
        <div ref={listRef} className="absolute z-50 w-full mt-1 bg-white border border-gray-300 rounded-lg shadow-lg max-h-48 overflow-y-auto">
          {isLoading ? (
            <div className="p-3 text-sm text-gray-500 flex items-center gap-2">
              <RefreshCw className="w-4 h-4 animate-spin" /> Searching...
            </div>
          ) : results.length === 0 ? (
            <div className="p-3 text-sm text-gray-500">No customers found</div>
          ) : (
            results.map((c, idx) => (
              <button
                key={c.account}
                type="button"
                className={`w-full text-left px-3 py-2 text-sm border-b border-gray-100 last:border-b-0 ${
                  idx === highlightedIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                }`}
                onClick={() => handleSelect(c)}
                onMouseEnter={() => setHighlightedIndex(idx)}
              >
                <span className="font-medium">{c.account}</span>
                <span className="text-gray-600 ml-2">{c.name}</span>
                {c.postcode && <span className="text-gray-400 ml-2 text-xs">{c.postcode}</span>}
              </button>
            ))
          )}
        </div>
      )}
    </div>
  );
}

type TabType = 'invoices' | 'pending' | 'history' | 'mandates';

interface Invoice {
  opera_account: string;
  customer_name: string;
  invoice_ref: string;
  invoice_date: string;
  due_date: string | null;
  days_until_due: number | null;
  amount: number;
  amount_formatted: string;
  original_amount?: number;
  is_overdue: boolean;
  is_due_by_advance?: boolean;
  has_mandate: boolean;
  mandate_id: string | null;
  mandate_status?: string | null;
  trans_type: string;
  trans_type_code?: number;
}

interface CustomerGroup {
  account: string;
  name: string;
  email: string | null;
  has_mandate: boolean;
  mandate_id: string | null;
  invoices: Invoice[];
  total_due: number;
  total_due_formatted: string;
  invoice_count: number;
}

interface DueInvoicesResponse {
  customers: CustomerGroup[];
  invoices: Invoice[];
  summary: {
    total_customers: number;
    total_invoices: number;
    total_amount: number;
    total_amount_formatted: string;
    collectable_amount: number;
    collectable_formatted: string;
    customers_with_mandate: number;
    customers_without_mandate: number;
  };
  advance_date: string;
  today: string;
}

interface PaymentRequest {
  id: number;
  payment_id: string | null;
  mandate_id: string;
  opera_account: string;
  customer_name?: string;
  amount_pence: number;
  amount_formatted: string;
  currency: string;
  charge_date: string | null;
  description: string | null;
  invoice_refs: string[];
  status: string;
  payout_id: string | null;
  opera_receipt_ref: string | null;
  error_message: string | null;
  created_at: string;
  updated_at: string | null;
}

interface Mandate {
  id: number;
  opera_account: string;
  opera_name: string | null;
  gocardless_customer_id: string | null;
  mandate_id: string;
  mandate_status: string;
  scheme: string;
  email: string | null;
  created_at: string;
  updated_at: string | null;
}

interface Stats {
  active_mandates: number;
  pending_count: number;
  pending_amount_formatted: string;
  month_collected_count: number;
  month_collected_formatted: string;
  failed_count_30d: number;
}

interface EligibleCustomer {
  account: string;
  name: string;
  balance: number;
  email: string | null;
  phone: string | null;
  contact: string | null;
  has_mandate: boolean;
  mandate_id: string | null;
  mandate_status: string | null;
}

export default function GoCardlessRequests() {
  const queryClient = useQueryClient();
  const [activeTab, setActiveTab] = useState<TabType>('invoices');
  const [selectedInvoices, setSelectedInvoices] = useState<Set<string>>(new Set());
  const [advanceDate, setAdvanceDate] = useState<string>(() => {
    // Default to 7 days from now
    const d = new Date();
    d.setDate(d.getDate() + 7);
    return d.toISOString().split('T')[0];
  });
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<string | null>(null);

  // Link mandate modal state
  const [showLinkModal, setShowLinkModal] = useState(false);
  const [linkOperaAccount, setLinkOperaAccount] = useState('');
  const [linkMandateId, setLinkMandateId] = useState('');
  const [linkOperaName, setLinkOperaName] = useState('');

  // Stats query - cached, only refresh on interval or explicit refetch
  const { data: statsData } = useQuery({
    queryKey: ['gocardless-payment-stats'],
    queryFn: async () => {
      const res = await authFetch('/api/gocardless/payment-requests/stats');
      const data = await res.json();
      if (!data.success) throw new Error(data.error);
      return data as Stats & { success: boolean };
    },
    staleTime: 30000,         // Cache for 30 seconds
    refetchInterval: 60000,   // Refresh every 60 seconds (was 30s)
    refetchOnWindowFocus: false,
  });

  // Due invoices query (GC customers only, with advance date) - cached by date
  const { data: dueInvoicesData, isLoading: loadingDueInvoices, refetch: refetchDueInvoices } = useQuery({
    queryKey: ['gocardless-due-invoices', advanceDate],
    queryFn: async () => {
      const params = new URLSearchParams();
      params.set('advance_date', advanceDate);
      params.set('gc_customers_only', 'true');
      const res = await authFetch(`/api/gocardless/due-invoices?${params}`);
      const data = await res.json();
      if (!data.success) throw new Error(data.error);
      return data as DueInvoicesResponse;
    },
    enabled: activeTab === 'invoices',
    staleTime: 2 * 60 * 1000,  // Cache for 2 minutes per date
    gcTime: 5 * 60 * 1000,
  });

  // Payment requests query - cached per status
  const { data: requestsData, isLoading: loadingRequests } = useQuery({
    queryKey: ['gocardless-payment-requests', activeTab],
    queryFn: async () => {
      const status = activeTab === 'pending' ? 'pending,pending_submission,submitted,confirmed' : undefined;
      const params = new URLSearchParams();
      if (status) params.set('status', status);
      const res = await authFetch(`/api/gocardless/payment-requests?${params}`);
      const data = await res.json();
      if (!data.success) throw new Error(data.error);
      return data as { requests: PaymentRequest[] };
    },
    enabled: activeTab === 'pending' || activeTab === 'history',
    staleTime: 60000,  // Cache for 1 minute
  });

  // Mandates query (linked to Opera) - cached, refresh on explicit action
  const { data: mandatesData, isLoading: loadingMandates, refetch: refetchMandates } = useQuery({
    queryKey: ['gocardless-mandates'],
    queryFn: async () => {
      const res = await authFetch('/api/gocardless/mandates');
      const data = await res.json();
      if (!data.success) throw new Error(data.error);
      // Filter out unlinked mandates (they have their own section)
      const linked = (data.mandates || []).filter((m: Mandate) => m.opera_account !== '__UNLINKED__');
      return { mandates: linked } as { mandates: Mandate[] };
    },
    enabled: activeTab === 'mandates',
    staleTime: 5 * 60 * 1000,  // Cache for 5 minutes
    gcTime: 10 * 60 * 1000,
  });

  // Unlinked mandates query (synced from GoCardless but not yet linked) - cached
  const { data: unlinkedMandatesData, isLoading: loadingUnlinked, refetch: refetchUnlinked } = useQuery({
    queryKey: ['gocardless-unlinked-mandates'],
    queryFn: async () => {
      const res = await authFetch('/api/gocardless/mandates/unlinked');
      const data = await res.json();
      if (!data.success) throw new Error(data.error);
      return data as { mandates: Mandate[]; count: number };
    },
    enabled: activeTab === 'mandates',
    staleTime: 5 * 60 * 1000,  // Cache for 5 minutes
    gcTime: 10 * 60 * 1000,
  });

  // Sync mandates mutation
  const syncMandatesMutation = useMutation({
    mutationFn: async () => {
      const res = await authFetch('/api/gocardless/mandates/sync', {
        method: 'POST'
      });
      return res.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        setSuccess(data.message || 'Mandates synced successfully');
        refetchMandates();
        refetchUnlinked();
        refetchEligible();
      } else {
        setError(data.error);
      }
    }
  });

  // Eligible customers query (customers with GC analysis code)
  // Cached with staleTime to avoid unnecessary refetches - only refresh on demand
  const { data: eligibleData, isLoading: loadingEligible, refetch: refetchEligible } = useQuery({
    queryKey: ['gocardless-eligible-customers'],
    queryFn: async () => {
      const res = await authFetch('/api/gocardless/eligible-customers');
      const data = await res.json();
      if (!data.success) throw new Error(data.error);
      return data as { customers: EligibleCustomer[]; count: number; with_mandate: number; without_mandate: number };
    },
    enabled: activeTab === 'mandates',
    staleTime: 5 * 60 * 1000,  // Cache for 5 minutes - don't refetch if data exists
    gcTime: 10 * 60 * 1000,   // Keep in memory for 10 minutes
  });

  // Request payment mutation
  const requestPaymentMutation = useMutation({
    mutationFn: async (params: { invoices: Invoice[] }) => {
      // Group by customer
      const byCustomer = params.invoices.reduce((acc, inv) => {
        if (!acc[inv.opera_account]) acc[inv.opera_account] = [];
        acc[inv.opera_account].push(inv);
        return acc;
      }, {} as Record<string, Invoice[]>);

      const requests = Object.entries(byCustomer).map(([account, invs]) => ({
        opera_account: account,
        invoices: invs.map(i => i.invoice_ref),
        amount: Math.round(invs.reduce((sum, i) => sum + i.amount, 0) * 100)
      }));

      const res = await authFetch('/api/gocardless/payment-requests/bulk', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ requests })
      });
      return res.json();
    },
    onSuccess: (data) => {
      if (data.success || data.summary?.succeeded > 0) {
        setSuccess(`Payment requested for ${data.summary?.succeeded || 0} customers`);
        setSelectedInvoices(new Set());
        queryClient.invalidateQueries({ queryKey: ['gocardless-payment-requests'] });
        queryClient.invalidateQueries({ queryKey: ['gocardless-payment-stats'] });
        refetchDueInvoices();
      } else {
        setError(data.error || 'Failed to request payments');
      }
    },
    onError: (err: Error) => setError(err.message)
  });

  // Cancel payment mutation
  const cancelPaymentMutation = useMutation({
    mutationFn: async (requestId: number) => {
      const res = await authFetch(`/api/gocardless/payment-requests/${requestId}/cancel`, {
        method: 'POST'
      });
      return res.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        setSuccess('Payment request cancelled');
        queryClient.invalidateQueries({ queryKey: ['gocardless-payment-requests'] });
        queryClient.invalidateQueries({ queryKey: ['gocardless-payment-stats'] });
      } else {
        setError(data.error);
      }
    }
  });

  // Link mandate mutation
  const linkMandateMutation = useMutation({
    mutationFn: async (params: { opera_account: string; mandate_id: string; opera_name?: string }) => {
      const res = await authFetch('/api/gocardless/mandates/link', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(params)
      });
      return res.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        setSuccess('Mandate linked successfully');
        setShowLinkModal(false);
        setLinkOperaAccount('');
        setLinkMandateId('');
        setLinkOperaName('');
        queryClient.invalidateQueries({ queryKey: ['gocardless-mandates'] });
        queryClient.invalidateQueries({ queryKey: ['gocardless-unlinked-mandates'] });
        queryClient.invalidateQueries({ queryKey: ['gocardless-eligible-customers'] });
        queryClient.invalidateQueries({ queryKey: ['gocardless-collectable-invoices'] });
      } else {
        setError(data.error);
      }
    }
  });

  // Unlink mandate mutation
  const unlinkMandateMutation = useMutation({
    mutationFn: async (mandateId: string) => {
      const res = await authFetch(`/api/gocardless/mandates/${mandateId}`, {
        method: 'DELETE'
      });
      return res.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        setSuccess('Mandate unlinked');
        queryClient.invalidateQueries({ queryKey: ['gocardless-mandates'] });
      } else {
        setError(data.error);
      }
    }
  });

  // Sync statuses mutation
  const syncStatusesMutation = useMutation({
    mutationFn: async () => {
      const res = await authFetch('/api/gocardless/payment-requests/sync', { method: 'POST' });
      return res.json();
    },
    onSuccess: (data) => {
      if (data.success) {
        setSuccess(`Synced ${data.updated} payment statuses`);
        queryClient.invalidateQueries({ queryKey: ['gocardless-payment-requests'] });
        queryClient.invalidateQueries({ queryKey: ['gocardless-payment-stats'] });
      } else {
        setError(data.error);
      }
    }
  });

  // Clear messages after delay
  useEffect(() => {
    if (success) {
      const timer = setTimeout(() => setSuccess(null), 5000);
      return () => clearTimeout(timer);
    }
  }, [success]);

  useEffect(() => {
    if (error) {
      const timer = setTimeout(() => setError(null), 10000);
      return () => clearTimeout(timer);
    }
  }, [error]);

  // Invoice selection helpers
  const toggleInvoice = (key: string) => {
    const newSet = new Set(selectedInvoices);
    if (newSet.has(key)) {
      newSet.delete(key);
    } else {
      newSet.add(key);
    }
    setSelectedInvoices(newSet);
  };

  // Normalize company name for matching
  const normalizeCompanyName = (name: string): string => {
    if (!name) return '';
    let n = name.toUpperCase().trim();
    // Remove common suffixes
    const suffixes = [' LTD', ' LIMITED', ' PLC', ' INC', ' LLC', ' CO', ' COMPANY', ' & CO', ' AND CO'];
    for (const suffix of suffixes) {
      if (n.endsWith(suffix)) {
        n = n.slice(0, -suffix.length);
      }
    }
    // Remove punctuation and extra spaces
    n = n.replace(/[.,]/g, '').replace(/\s+/g, ' ').trim();
    return n;
  };

  // Find matching Opera customer by name for auto-linking
  const findMatchingOperaCustomer = (gcCustomerName: string | null): EligibleCustomer | null => {
    if (!gcCustomerName) {
      return null;
    }

    if (!eligibleData?.customers || eligibleData.customers.length === 0) {
      return null;
    }

    const normGC = normalizeCompanyName(gcCustomerName);

    // Search ALL eligible customers (not just those without mandates)
    // User can decide if they want to re-link
    const allCustomers = eligibleData.customers;

    // Exact match after normalization
    for (const customer of allCustomers) {
      const normOpera = normalizeCompanyName(customer.name);
      if (normOpera === normGC) {
        return customer;
      }
    }

    // Partial match (one contains the other)
    for (const customer of allCustomers) {
      const normOpera = normalizeCompanyName(customer.name);
      if (normGC.includes(normOpera) || normOpera.includes(normGC)) {
        return customer;
      }
    }

    return null;
  };

  const toggleCustomer = (account: string) => {
    const customer = dueInvoicesData?.customers.find(c => c.account === account);
    if (!customer || !customer.has_mandate) return;

    const newSet = new Set(selectedInvoices);
    const customerKeys = customer.invoices.map(i => `${i.opera_account}:${i.invoice_ref}`);
    const allSelected = customerKeys.every(k => newSet.has(k));

    if (allSelected) {
      // Deselect all
      customerKeys.forEach(k => newSet.delete(k));
    } else {
      // Select all
      customerKeys.forEach(k => newSet.add(k));
    }
    setSelectedInvoices(newSet);
  };

  const selectAllWithMandate = () => {
    const keys = (dueInvoicesData?.invoices || [])
      .filter(i => i.has_mandate)
      .map(i => `${i.opera_account}:${i.invoice_ref}`);
    setSelectedInvoices(new Set(keys));
  };

  const getSelectedInvoices = (): Invoice[] => {
    return (dueInvoicesData?.invoices || []).filter(
      i => selectedInvoices.has(`${i.opera_account}:${i.invoice_ref}`)
    );
  };

  const selectedTotal = getSelectedInvoices().reduce((sum, i) => sum + i.amount, 0);

  const isCustomerFullySelected = (account: string): boolean => {
    const customer = dueInvoicesData?.customers.find(c => c.account === account);
    if (!customer) return false;
    return customer.invoices.every(i => selectedInvoices.has(`${i.opera_account}:${i.invoice_ref}`));
  };

  const isCustomerPartiallySelected = (account: string): boolean => {
    const customer = dueInvoicesData?.customers.find(c => c.account === account);
    if (!customer) return false;
    const selected = customer.invoices.filter(i => selectedInvoices.has(`${i.opera_account}:${i.invoice_ref}`));
    return selected.length > 0 && selected.length < customer.invoices.length;
  };

  const getStatusBadge = (status: string) => {
    const badges: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
      pending: { color: 'bg-yellow-100 text-yellow-800', icon: <Clock className="w-3 h-3" />, label: 'Pending' },
      pending_submission: { color: 'bg-yellow-100 text-yellow-800', icon: <Clock className="w-3 h-3" />, label: 'Awaiting Submission' },
      pending_customer_approval: { color: 'bg-blue-100 text-blue-800', icon: <Clock className="w-3 h-3" />, label: 'Customer Approval' },
      submitted: { color: 'bg-blue-100 text-blue-800', icon: <Send className="w-3 h-3" />, label: 'Submitted' },
      confirmed: { color: 'bg-green-100 text-green-800', icon: <CheckCircle className="w-3 h-3" />, label: 'Confirmed' },
      paid_out: { color: 'bg-green-100 text-green-800', icon: <CheckCircle className="w-3 h-3" />, label: 'Paid Out' },
      failed: { color: 'bg-red-100 text-red-800', icon: <AlertCircle className="w-3 h-3" />, label: 'Failed' },
      cancelled: { color: 'bg-gray-100 text-gray-800', icon: <Ban className="w-3 h-3" />, label: 'Cancelled' }
    };
    const badge = badges[status] || { color: 'bg-gray-100 text-gray-800', icon: null, label: status };
    return (
      <span className={`inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium ${badge.color}`}>
        {badge.icon}
        {badge.label}
      </span>
    );
  };

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-green-100 rounded-lg">
            <CreditCard className="w-6 h-6 text-green-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">GoCardless Payment Requests</h1>
            <p className="text-sm text-gray-500">Request Direct Debit payments from customers</p>
          </div>
        </div>
        <button
          onClick={() => syncStatusesMutation.mutate()}
          disabled={syncStatusesMutation.isPending}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 ${syncStatusesMutation.isPending ? 'animate-spin' : ''}`} />
          Sync Status
        </button>
      </div>

      {/* Messages */}
      {error && (
        <div className="mb-4 p-4 bg-red-50 border border-red-200 rounded-lg flex items-start gap-3">
          <AlertCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="text-sm text-red-800">{error}</p>
          </div>
          <button onClick={() => setError(null)} className="text-red-400 hover:text-red-600">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {success && (
        <div className="mb-4 p-4 bg-green-50 border border-green-200 rounded-lg flex items-start gap-3">
          <CheckCircle className="w-5 h-5 text-green-500 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="text-sm text-green-800">{success}</p>
          </div>
          <button onClick={() => setSuccess(null)} className="text-green-400 hover:text-green-600">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Stats Summary */}
      {statsData && (
        <div className="grid grid-cols-4 gap-4 mb-6">
          <div className="bg-white p-4 rounded-lg border border-gray-200">
            <div className="flex items-center gap-2 text-gray-500 text-sm mb-1">
              <Users className="w-4 h-4" />
              Active Mandates
            </div>
            <div className="text-2xl font-semibold text-gray-900">{statsData.active_mandates}</div>
          </div>
          <div className="bg-white p-4 rounded-lg border border-gray-200">
            <div className="flex items-center gap-2 text-gray-500 text-sm mb-1">
              <Clock className="w-4 h-4" />
              Pending
            </div>
            <div className="text-2xl font-semibold text-yellow-600">{statsData.pending_amount_formatted}</div>
            <div className="text-xs text-gray-500">{statsData.pending_count} payments</div>
          </div>
          <div className="bg-white p-4 rounded-lg border border-gray-200">
            <div className="flex items-center gap-2 text-gray-500 text-sm mb-1">
              <CheckCircle className="w-4 h-4" />
              This Month
            </div>
            <div className="text-2xl font-semibold text-green-600">{statsData.month_collected_formatted}</div>
            <div className="text-xs text-gray-500">{statsData.month_collected_count} collected</div>
          </div>
          <div className="bg-white p-4 rounded-lg border border-gray-200">
            <div className="flex items-center gap-2 text-gray-500 text-sm mb-1">
              <AlertCircle className="w-4 h-4" />
              Failed (30d)
            </div>
            <div className="text-2xl font-semibold text-red-600">{statsData.failed_count_30d}</div>
          </div>
        </div>
      )}

      {/* Tabs */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="border-b border-gray-200">
          <nav className="flex">
            {[
              { id: 'invoices', label: 'Outstanding Invoices', icon: FileText },
              { id: 'pending', label: 'Pending Requests', icon: Clock },
              { id: 'history', label: 'Payment History', icon: History },
              { id: 'mandates', label: 'Mandates', icon: Link }
            ].map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as TabType)}
                className={`flex items-center gap-2 px-6 py-3 text-sm font-medium border-b-2 -mb-px ${
                  activeTab === tab.id
                    ? 'border-green-500 text-green-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                <tab.icon className="w-4 h-4" />
                {tab.label}
              </button>
            ))}
          </nav>
        </div>

        {/* Tab Content */}
        <div className="p-4">
          {/* Outstanding Invoices Tab */}
          {activeTab === 'invoices' && (
            <div>
              {/* Advance Date Selector and Filters */}
              <div className="bg-gray-50 rounded-lg p-4 mb-4">
                <div className="flex flex-wrap items-center gap-4">
                  <div className="flex items-center gap-2">
                    <label className="text-sm font-medium text-gray-700">Show invoices due by:</label>
                    <input
                      type="date"
                      value={advanceDate}
                      onChange={e => setAdvanceDate(e.target.value)}
                      className="px-3 py-1.5 border border-gray-300 rounded-lg text-sm focus:ring-green-500 focus:border-green-500"
                    />
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => {
                        const d = new Date();
                        setAdvanceDate(d.toISOString().split('T')[0]);
                      }}
                      className="px-2 py-1 text-xs bg-white border border-gray-300 rounded hover:bg-gray-50"
                    >
                      Today
                    </button>
                    <button
                      onClick={() => {
                        const d = new Date();
                        d.setDate(d.getDate() + 7);
                        setAdvanceDate(d.toISOString().split('T')[0]);
                      }}
                      className="px-2 py-1 text-xs bg-white border border-gray-300 rounded hover:bg-gray-50"
                    >
                      +7 days
                    </button>
                    <button
                      onClick={() => {
                        const d = new Date();
                        d.setDate(d.getDate() + 14);
                        setAdvanceDate(d.toISOString().split('T')[0]);
                      }}
                      className="px-2 py-1 text-xs bg-white border border-gray-300 rounded hover:bg-gray-50"
                    >
                      +14 days
                    </button>
                    <button
                      onClick={() => {
                        const d = new Date();
                        d.setMonth(d.getMonth() + 1);
                        setAdvanceDate(d.toISOString().split('T')[0]);
                      }}
                      className="px-2 py-1 text-xs bg-white border border-gray-300 rounded hover:bg-gray-50"
                    >
                      +1 month
                    </button>
                  </div>
                  <div className="ml-auto flex items-center gap-2">
                    <button
                      onClick={selectAllWithMandate}
                      className="px-3 py-1.5 text-sm text-gray-700 border border-gray-300 rounded-lg hover:bg-gray-50 bg-white"
                    >
                      Select All
                    </button>
                    <button
                      onClick={() => refetchDueInvoices()}
                      className="p-2 text-gray-500 hover:text-gray-700"
                    >
                      <RefreshCw className="w-4 h-4" />
                    </button>
                  </div>
                </div>

                {/* Summary */}
                {dueInvoicesData?.summary && (
                  <div className="flex flex-wrap gap-4 mt-3 pt-3 border-t border-gray-200 text-sm">
                    <span className="text-gray-600">
                      <span className="font-medium text-gray-900">{dueInvoicesData.summary.total_customers}</span> customers
                    </span>
                    <span className="text-gray-600">
                      <span className="font-medium text-gray-900">{dueInvoicesData.summary.total_invoices}</span> invoices
                    </span>
                    <span className="text-gray-600">
                      Total: <span className="font-medium text-gray-900">{dueInvoicesData.summary.total_amount_formatted}</span>
                    </span>
                    <span className="text-green-600">
                      Collectable (with mandate): <span className="font-medium">{dueInvoicesData.summary.collectable_formatted}</span>
                    </span>
                    {dueInvoicesData.summary.customers_without_mandate > 0 && (
                      <span className="text-amber-600">
                        {dueInvoicesData.summary.customers_without_mandate} customers need mandate setup
                      </span>
                    )}
                  </div>
                )}
              </div>

              {/* Customers with Invoices */}
              {loadingDueInvoices ? (
                <div className="flex items-center justify-center py-12">
                  <RefreshCw className="w-6 h-6 text-gray-400 animate-spin" />
                </div>
              ) : dueInvoicesData?.customers.length === 0 ? (
                <div className="text-center py-12 text-gray-500">
                  <FileText className="w-12 h-12 mx-auto mb-3 text-gray-300" />
                  <p>No invoices due by {advanceDate}</p>
                  <p className="text-sm mt-1">Try selecting a later date or check that customers have 'GC' analysis code in Opera</p>
                </div>
              ) : (
                <div className="space-y-4">
                  {dueInvoicesData?.customers.map(customer => {
                    const isFullySelected = isCustomerFullySelected(customer.account);
                    const isPartiallySelected = isCustomerPartiallySelected(customer.account);
                    return (
                      <div
                        key={customer.account}
                        className={`border rounded-lg overflow-hidden ${
                          isFullySelected ? 'border-green-300 bg-green-50' :
                          isPartiallySelected ? 'border-green-200' : 'border-gray-200'
                        }`}
                      >
                        {/* Customer Header */}
                        <div
                          className={`flex items-center justify-between p-3 cursor-pointer ${
                            customer.has_mandate ? 'hover:bg-gray-50' : ''
                          }`}
                          onClick={() => customer.has_mandate && toggleCustomer(customer.account)}
                        >
                          <div className="flex items-center gap-3">
                            <input
                              type="checkbox"
                              checked={isFullySelected}
                              ref={el => {
                                if (el) el.indeterminate = isPartiallySelected;
                              }}
                              onChange={() => toggleCustomer(customer.account)}
                              disabled={!customer.has_mandate}
                              className="rounded border-gray-300 text-green-600 focus:ring-green-500 disabled:opacity-50"
                            />
                            <div>
                              <div className="font-medium text-gray-900">{customer.name}</div>
                              <div className="text-xs text-gray-500">
                                {customer.account} • {customer.invoice_count} invoice{customer.invoice_count !== 1 ? 's' : ''}
                                {customer.email && ` • ${customer.email}`}
                              </div>
                            </div>
                          </div>
                          <div className="flex items-center gap-4">
                            <div className="text-right">
                              <div className="font-medium text-gray-900">{customer.total_due_formatted}</div>
                            </div>
                            {customer.has_mandate ? (
                              <span className="inline-flex items-center gap-1 px-2 py-1 bg-green-100 text-green-700 rounded text-xs">
                                <CheckCircle className="w-3 h-3" />
                                DD Ready
                              </span>
                            ) : (
                              <button
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setLinkOperaAccount(customer.account);
                                  setLinkOperaName(customer.name);
                                  setShowLinkModal(true);
                                }}
                                className="inline-flex items-center gap-1 px-2 py-1 bg-amber-100 text-amber-700 rounded text-xs hover:bg-amber-200"
                              >
                                <Plus className="w-3 h-3" />
                                Link Mandate
                              </button>
                            )}
                          </div>
                        </div>

                        {/* Invoices Table */}
                        <div className="border-t border-gray-200">
                          <table className="min-w-full divide-y divide-gray-200">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="w-10 px-3 py-2"></th>
                                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Invoice</th>
                                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Date</th>
                                <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Due Date</th>
                                <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">Amount</th>
                              </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-gray-100">
                              {customer.invoices.map(invoice => {
                                const key = `${invoice.opera_account}:${invoice.invoice_ref}`;
                                const isSelected = selectedInvoices.has(key);
                                return (
                                  <tr
                                    key={key}
                                    className={`${isSelected ? 'bg-green-50' : ''} ${!customer.has_mandate ? 'opacity-60' : ''}`}
                                  >
                                    <td className="px-3 py-2">
                                      <input
                                        type="checkbox"
                                        checked={isSelected}
                                        onChange={() => toggleInvoice(key)}
                                        disabled={!customer.has_mandate}
                                        className="rounded border-gray-300 text-green-600 focus:ring-green-500 disabled:opacity-50"
                                      />
                                    </td>
                                    <td className="px-3 py-2 text-sm text-gray-900">{invoice.invoice_ref}</td>
                                    <td className="px-3 py-2 text-sm text-gray-500">{invoice.invoice_date}</td>
                                    <td className="px-3 py-2">
                                      {invoice.is_overdue ? (
                                        <span className="text-sm text-red-600 font-medium">
                                          {Math.abs(invoice.days_until_due || 0)} days overdue
                                        </span>
                                      ) : invoice.due_date ? (
                                        <span className="text-sm text-gray-500">
                                          {invoice.due_date}
                                          {invoice.days_until_due !== null && invoice.days_until_due > 0 && (
                                            <span className="text-gray-400 ml-1">({invoice.days_until_due}d)</span>
                                          )}
                                        </span>
                                      ) : (
                                        <span className="text-sm text-gray-400">-</span>
                                      )}
                                    </td>
                                    <td className="px-3 py-2 text-right text-sm font-medium text-gray-900">
                                      {invoice.amount_formatted}
                                    </td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}

              {/* Selection Actions */}
              {selectedInvoices.size > 0 && (
                <div className="mt-4 p-4 bg-green-50 border border-green-200 rounded-lg flex items-center justify-between sticky bottom-4">
                  <div>
                    <span className="text-sm font-medium text-green-800">
                      {selectedInvoices.size} invoice{selectedInvoices.size > 1 ? 's' : ''} selected
                    </span>
                    <span className="text-sm text-green-700 ml-2">
                      (Total: {'\u00A3'}{selectedTotal.toFixed(2)})
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => setSelectedInvoices(new Set())}
                      className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800"
                    >
                      Clear
                    </button>
                    <button
                      onClick={() => requestPaymentMutation.mutate({ invoices: getSelectedInvoices() })}
                      disabled={requestPaymentMutation.isPending}
                      className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white text-sm font-medium rounded-lg hover:bg-green-700 disabled:opacity-50"
                    >
                      {requestPaymentMutation.isPending ? (
                        <RefreshCw className="w-4 h-4 animate-spin" />
                      ) : (
                        <Send className="w-4 h-4" />
                      )}
                      Request Payment
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Pending Requests Tab */}
          {activeTab === 'pending' && (
            <div>
              {loadingRequests ? (
                <div className="flex items-center justify-center py-12">
                  <RefreshCw className="w-6 h-6 text-gray-400 animate-spin" />
                </div>
              ) : (
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Customer</th>
                      <th className="px-3 py-3 text-right text-xs font-medium text-gray-500 uppercase">Amount</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Requested</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Charge Date</th>
                      <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Status</th>
                      <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {requestsData?.requests
                      .filter(r => ['pending', 'pending_submission', 'submitted', 'confirmed'].includes(r.status))
                      .map(req => (
                        <tr key={req.id}>
                          <td className="px-3 py-3">
                            <div className="text-sm font-medium text-gray-900">{req.customer_name || req.opera_account}</div>
                            <div className="text-xs text-gray-500">{req.invoice_refs.join(', ')}</div>
                          </td>
                          <td className="px-3 py-3 text-right text-sm font-medium">{req.amount_formatted}</td>
                          <td className="px-3 py-3 text-sm text-gray-500">
                            {new Date(req.created_at).toLocaleDateString()}
                          </td>
                          <td className="px-3 py-3 text-sm text-gray-500">{req.charge_date || '-'}</td>
                          <td className="px-3 py-3 text-center">{getStatusBadge(req.status)}</td>
                          <td className="px-3 py-3 text-center">
                            {['pending', 'pending_submission'].includes(req.status) && (
                              <button
                                onClick={() => cancelPaymentMutation.mutate(req.id)}
                                disabled={cancelPaymentMutation.isPending}
                                className="text-red-600 hover:text-red-800 text-sm"
                              >
                                Cancel
                              </button>
                            )}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              )}
              {requestsData?.requests.filter(r => ['pending', 'pending_submission', 'submitted', 'confirmed'].includes(r.status)).length === 0 && (
                <div className="text-center py-12 text-gray-500">
                  No pending payment requests
                </div>
              )}
            </div>
          )}

          {/* Payment History Tab */}
          {activeTab === 'history' && (
            <div>
              {loadingRequests ? (
                <div className="flex items-center justify-center py-12">
                  <RefreshCw className="w-6 h-6 text-gray-400 animate-spin" />
                </div>
              ) : (
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Customer</th>
                      <th className="px-3 py-3 text-right text-xs font-medium text-gray-500 uppercase">Amount</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                      <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Status</th>
                      <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Receipt Ref</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {requestsData?.requests.map(req => (
                      <tr key={req.id}>
                        <td className="px-3 py-3">
                          <div className="text-sm font-medium text-gray-900">{req.customer_name || req.opera_account}</div>
                          <div className="text-xs text-gray-500">{req.invoice_refs.join(', ')}</div>
                        </td>
                        <td className="px-3 py-3 text-right text-sm font-medium">{req.amount_formatted}</td>
                        <td className="px-3 py-3 text-sm text-gray-500">
                          {new Date(req.created_at).toLocaleDateString()}
                        </td>
                        <td className="px-3 py-3 text-center">{getStatusBadge(req.status)}</td>
                        <td className="px-3 py-3 text-sm text-gray-500">
                          {req.opera_receipt_ref || (req.error_message ? (
                            <span className="text-red-500 text-xs">{req.error_message}</span>
                          ) : '-')}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}

          {/* Mandates Tab */}
          {activeTab === 'mandates' && (
            <div className="space-y-6">
              {/* Eligible Customers Section - Customers with GC analysis code */}
              <div className="border-b border-gray-200 pb-6">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                      <Users className="w-5 h-5 text-green-600" />
                      Eligible Customers
                    </h3>
                    <p className="text-sm text-gray-500">
                      Customers with 'GC' analysis code in Opera. Set sn_analsys='GC' on a customer record to add them here.
                    </p>
                  </div>
                  <button
                    onClick={() => refetchEligible()}
                    className="flex items-center gap-2 px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800"
                  >
                    <RefreshCw className="w-4 h-4" />
                    Refresh
                  </button>
                </div>

                {loadingEligible ? (
                  <div className="flex items-center justify-center py-8">
                    <RefreshCw className="w-5 h-5 text-gray-400 animate-spin" />
                  </div>
                ) : eligibleData?.customers && eligibleData.customers.length > 0 ? (
                  <>
                    <div className="flex gap-4 mb-3 text-sm">
                      <span className="text-gray-600">
                        <span className="font-medium text-gray-900">{eligibleData.count}</span> eligible customers
                      </span>
                      <span className="text-green-600">
                        <span className="font-medium">{eligibleData.with_mandate}</span> with mandate
                      </span>
                      <span className="text-amber-600">
                        <span className="font-medium">{eligibleData.without_mandate}</span> need mandate
                      </span>
                    </div>
                    <div className="overflow-hidden rounded-lg border border-gray-200">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Account</th>
                            <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Customer Name</th>
                            <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase">Balance</th>
                            <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Email</th>
                            <th className="px-3 py-2 text-center text-xs font-medium text-gray-500 uppercase">Status</th>
                            <th className="px-3 py-2 text-center text-xs font-medium text-gray-500 uppercase">Action</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-200 bg-white">
                          {eligibleData.customers.map(customer => (
                            <tr key={customer.account} className={customer.has_mandate ? 'bg-green-50' : ''}>
                              <td className="px-3 py-2 text-sm font-medium text-gray-900">{customer.account}</td>
                              <td className="px-3 py-2 text-sm text-gray-700">{customer.name}</td>
                              <td className="px-3 py-2 text-sm text-right text-gray-700">
                                £{customer.balance.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                              </td>
                              <td className="px-3 py-2 text-sm text-gray-500">{customer.email || '-'}</td>
                              <td className="px-3 py-2 text-center">
                                {customer.has_mandate ? (
                                  <span className="inline-flex items-center gap-1 px-2 py-1 text-xs rounded-full bg-green-100 text-green-800">
                                    <CheckCircle className="w-3 h-3" />
                                    Linked
                                  </span>
                                ) : (
                                  <span className="inline-flex items-center gap-1 px-2 py-1 text-xs rounded-full bg-amber-100 text-amber-800">
                                    <AlertCircle className="w-3 h-3" />
                                    No Mandate
                                  </span>
                                )}
                              </td>
                              <td className="px-3 py-2 text-center">
                                {!customer.has_mandate && (
                                  <button
                                    onClick={() => {
                                      setLinkOperaAccount(customer.account);
                                      setLinkOperaName(customer.name);
                                      setLinkMandateId('');
                                      setShowLinkModal(true);
                                    }}
                                    className="inline-flex items-center gap-1 px-2 py-1 text-xs font-medium text-green-700 bg-green-100 rounded hover:bg-green-200"
                                  >
                                    <Link className="w-3 h-3" />
                                    Link
                                  </button>
                                )}
                                {customer.has_mandate && (
                                  <span className="text-xs text-gray-400 font-mono">{customer.mandate_id}</span>
                                )}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </>
                ) : (
                  <div className="text-center py-8 text-gray-500 bg-gray-50 rounded-lg">
                    <Users className="w-8 h-8 mx-auto mb-2 text-gray-300" />
                    <p>No customers with 'GC' analysis code found.</p>
                    <p className="text-sm mt-1">Set sn_analsys='GC' on customer records in Opera to enable GoCardless.</p>
                  </div>
                )}
              </div>

              {/* Sync from GoCardless Section */}
              <div className="border-b border-gray-200 pb-6">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                      <RefreshCw className="w-5 h-5 text-blue-600" />
                      GoCardless Mandates
                    </h3>
                    <p className="text-sm text-gray-500">
                      Sync active mandates from your GoCardless account, then link them to Opera customers.
                    </p>
                  </div>
                  <button
                    onClick={() => syncMandatesMutation.mutate()}
                    disabled={syncMandatesMutation.isPending}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50"
                  >
                    <RefreshCw className={`w-4 h-4 ${syncMandatesMutation.isPending ? 'animate-spin' : ''}`} />
                    {syncMandatesMutation.isPending ? 'Syncing...' : 'Sync from GoCardless'}
                  </button>
                </div>

                {/* Unlinked mandates from GoCardless */}
                {loadingUnlinked ? (
                  <div className="flex items-center justify-center py-4">
                    <RefreshCw className="w-4 h-4 text-gray-400 animate-spin" />
                  </div>
                ) : unlinkedMandatesData?.mandates && unlinkedMandatesData.mandates.length > 0 ? (
                  <div>
                    <p className="text-sm text-amber-600 mb-3">
                      <span className="font-medium">{unlinkedMandatesData.count}</span> mandates from GoCardless need linking to Opera customers:
                    </p>
                    <div className="overflow-hidden rounded-lg border border-amber-200 bg-amber-50">
                      <table className="min-w-full divide-y divide-amber-200">
                        <thead className="bg-amber-100">
                          <tr>
                            <th className="px-3 py-2 text-left text-xs font-medium text-amber-800 uppercase">Mandate ID</th>
                            <th className="px-3 py-2 text-left text-xs font-medium text-amber-800 uppercase">GoCardless Customer</th>
                            <th className="px-3 py-2 text-left text-xs font-medium text-amber-800 uppercase">Email</th>
                            <th className="px-3 py-2 text-center text-xs font-medium text-amber-800 uppercase">Scheme</th>
                            <th className="px-3 py-2 text-center text-xs font-medium text-amber-800 uppercase">Link to Opera</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-amber-200">
                          {unlinkedMandatesData.mandates.map(mandate => (
                            <tr key={mandate.mandate_id}>
                              <td className="px-3 py-2 text-sm font-mono text-gray-900">{mandate.mandate_id}</td>
                              <td className="px-3 py-2 text-sm text-gray-700">{mandate.opera_name || '-'}</td>
                              <td className="px-3 py-2 text-sm text-gray-500">{mandate.email || '-'}</td>
                              <td className="px-3 py-2 text-center text-sm text-gray-500 uppercase">{mandate.scheme}</td>
                              <td className="px-3 py-2 text-center">
                                <button
                                  onClick={() => {
                                    setLinkMandateId(mandate.mandate_id);
                                    setLinkOperaName(mandate.opera_name || '');
                                    // Auto-find matching Opera customer by name
                                    const match = findMatchingOperaCustomer(mandate.opera_name);
                                    setLinkOperaAccount(match?.account || '');
                                    setShowLinkModal(true);
                                  }}
                                  className="px-3 py-1 text-xs bg-green-600 text-white rounded hover:bg-green-700"
                                >
                                  Link
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : (
                  <p className="text-sm text-gray-500">
                    No unlinked mandates. Click "Sync from GoCardless" to fetch mandates from your GoCardless account.
                  </p>
                )}
              </div>

              {/* All Linked Mandates Section */}
              <div>
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                    <CreditCard className="w-5 h-5 text-blue-600" />
                    Linked Mandates
                  </h3>
                  <button
                    onClick={() => setShowLinkModal(true)}
                    className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white text-sm font-medium rounded-lg hover:bg-green-700"
                  >
                    <Plus className="w-4 h-4" />
                    Link Mandate Manually
                  </button>
                </div>

                {loadingMandates ? (
                  <div className="flex items-center justify-center py-12">
                    <RefreshCw className="w-6 h-6 text-gray-400 animate-spin" />
                  </div>
                ) : mandatesData?.mandates && mandatesData.mandates.length > 0 ? (
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Opera Account</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Customer Name</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Mandate ID</th>
                        <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Status</th>
                        <th className="px-3 py-3 text-left text-xs font-medium text-gray-500 uppercase">Scheme</th>
                        <th className="px-3 py-3 text-center text-xs font-medium text-gray-500 uppercase">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-200">
                      {mandatesData.mandates.map(mandate => (
                        <tr key={mandate.id}>
                          <td className="px-3 py-3 text-sm font-medium text-gray-900">{mandate.opera_account}</td>
                          <td className="px-3 py-3 text-sm text-gray-700">{mandate.opera_name || '-'}</td>
                          <td className="px-3 py-3 text-sm text-gray-500 font-mono">{mandate.mandate_id}</td>
                          <td className="px-3 py-3 text-center">
                            <span className={`inline-flex px-2 py-1 text-xs rounded-full ${
                              mandate.mandate_status === 'active'
                                ? 'bg-green-100 text-green-800'
                                : 'bg-gray-100 text-gray-800'
                            }`}>
                              {mandate.mandate_status}
                            </span>
                          </td>
                          <td className="px-3 py-3 text-sm text-gray-500 uppercase">{mandate.scheme}</td>
                          <td className="px-3 py-3 text-center">
                            <button
                              onClick={() => unlinkMandateMutation.mutate(mandate.mandate_id)}
                              className="text-red-600 hover:text-red-800"
                              title="Unlink mandate"
                            >
                              <Unlink className="w-4 h-4" />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <div className="text-center py-12 text-gray-500">
                    No mandates linked. Click "Link Mandate" to add one.
                  </div>
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Link Mandate Modal */}
      {showLinkModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-full max-w-md p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold">Link GoCardless Mandate</h3>
              <button onClick={() => setShowLinkModal(false)} className="text-gray-400 hover:text-gray-600">
                <X className="w-5 h-5" />
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Opera Account Code
                </label>
                <CustomerAccountSearch
                  value={linkOperaAccount}
                  valueName={linkOperaName}
                  onChange={(account, name) => {
                    setLinkOperaAccount(account);
                    setLinkOperaName(name);
                  }}
                  placeholder="Type to search Opera customers..."
                />
                <p className="text-xs text-gray-500 mt-1">
                  Search by account code or customer name
                </p>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Customer Name
                </label>
                <input
                  type="text"
                  value={linkOperaName}
                  readOnly
                  placeholder="Selected from search above"
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg bg-gray-50 text-gray-600"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  GoCardless Mandate ID
                </label>
                <input
                  type="text"
                  value={linkMandateId}
                  onChange={e => setLinkMandateId(e.target.value)}
                  placeholder="e.g., MD00XXXXXXXX"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-green-500 focus:border-green-500"
                />
                <p className="text-xs text-gray-500 mt-1">
                  Find mandate IDs in your GoCardless dashboard under Customers &gt; Mandates
                </p>
              </div>

              {/* Show eligible customers for quick selection */}
              {eligibleData?.customers && eligibleData.customers.length > 0 && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Or select from eligible customers:
                  </label>
                  <select
                    value={linkOperaAccount}
                    onChange={e => {
                      if (!e.target.value) return;
                      const selected = eligibleData.customers.find(c => c.account === e.target.value);
                      if (selected) {
                        setLinkOperaAccount(selected.account);
                        if (!linkOperaName) {
                          setLinkOperaName(selected.name);
                        }
                      }
                    }}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-green-500 focus:border-green-500"
                  >
                    <option value="">-- Select customer --</option>
                    {eligibleData.customers
                      .sort((a, b) => a.name.localeCompare(b.name))
                      .map(c => (
                        <option
                          key={c.account}
                          value={c.account}
                          disabled={c.has_mandate}
                        >
                          {c.account} - {c.name}{c.has_mandate ? ' (already linked)' : ''}
                        </option>
                      ))
                    }
                  </select>
                  {eligibleData.customers.filter(c => !c.has_mandate).length === 0 && (
                    <p className="text-xs text-amber-600 mt-1">All eligible customers already have mandates linked.</p>
                  )}
                </div>
              )}
              {!eligibleData?.customers && (
                <p className="text-xs text-gray-500">Loading eligible customers...</p>
              )}
            </div>

            <div className="flex justify-end gap-3 mt-6">
              <button
                onClick={() => setShowLinkModal(false)}
                className="px-4 py-2 text-sm text-gray-700 border border-gray-300 rounded-lg hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={() => linkMandateMutation.mutate({
                  opera_account: linkOperaAccount,
                  mandate_id: linkMandateId,
                  opera_name: linkOperaName || undefined
                })}
                disabled={!linkOperaAccount || !linkMandateId || linkMandateMutation.isPending}
                className="px-4 py-2 text-sm font-medium text-white bg-green-600 rounded-lg hover:bg-green-700 disabled:opacity-50"
              >
                {linkMandateMutation.isPending ? 'Linking...' : 'Link Mandate'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
