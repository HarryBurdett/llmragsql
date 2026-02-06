import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Mail,
  Inbox,
  RefreshCw,
  Search,
  Filter,
  User,
  Clock,
  Paperclip,
  AlertCircle,
  CheckCircle,
  XCircle,
  Link,
  Unlink,
} from 'lucide-react';
import apiClient from '../api/client';
import type { Email as EmailType, EmailListParams } from '../api/client';

const CATEGORIES = [
  { value: 'payment', label: 'Payment', color: 'bg-green-100 text-green-800' },
  { value: 'query', label: 'Query', color: 'bg-blue-100 text-blue-800' },
  { value: 'complaint', label: 'Complaint', color: 'bg-red-100 text-red-800' },
  { value: 'order', label: 'Order', color: 'bg-purple-100 text-purple-800' },
  { value: 'other', label: 'Other', color: 'bg-gray-100 text-gray-800' },
  { value: 'uncategorized', label: 'Uncategorized', color: 'bg-yellow-100 text-yellow-800' },
];

function getCategoryStyle(category: string | null) {
  const cat = CATEGORIES.find((c) => c.value === category);
  return cat?.color || 'bg-gray-100 text-gray-800';
}

function getCategoryLabel(category: string | null) {
  const cat = CATEGORIES.find((c) => c.value === category);
  return cat?.label || 'Uncategorized';
}

function formatDate(dateStr: string) {
  const date = new Date(dateStr);
  const now = new Date();
  const diffDays = Math.floor((now.getTime() - date.getTime()) / (1000 * 60 * 60 * 24));

  if (diffDays === 0) {
    return date.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' });
  } else if (diffDays === 1) {
    return 'Yesterday';
  } else if (diffDays < 7) {
    return date.toLocaleDateString('en-GB', { weekday: 'short' });
  } else {
    return date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
  }
}

export function Email() {
  const queryClient = useQueryClient();
  const [selectedEmail, setSelectedEmail] = useState<EmailType | null>(null);
  const [filters, setFilters] = useState<EmailListParams>({
    page: 1,
    page_size: 50,
  });
  const [searchTerm, setSearchTerm] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  const [linkAccountInput, setLinkAccountInput] = useState('');

  // Fetch providers
  const providersQuery = useQuery({
    queryKey: ['emailProviders'],
    queryFn: async () => {
      const response = await apiClient.emailProviders();
      return response.data;
    },
  });

  // Fetch emails
  const emailsQuery = useQuery({
    queryKey: ['emails', filters],
    queryFn: async () => {
      const response = await apiClient.emailMessages(filters);
      return response.data;
    },
  });

  // Fetch email stats
  const statsQuery = useQuery({
    queryKey: ['emailStats'],
    queryFn: async () => {
      const response = await apiClient.emailStats();
      return response.data;
    },
  });

  // Fetch sync status
  const syncStatusQuery = useQuery({
    queryKey: ['emailSyncStatus'],
    queryFn: async () => {
      const response = await apiClient.emailSyncStatus();
      return response.data;
    },
    refetchInterval: 10000,
  });

  // Sync mutation
  const syncMutation = useMutation({
    mutationFn: async () => {
      const response = await apiClient.emailSync();
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['emails'] });
      queryClient.invalidateQueries({ queryKey: ['emailStats'] });
      queryClient.invalidateQueries({ queryKey: ['emailSyncStatus'] });
    },
  });

  // Update category mutation
  const updateCategoryMutation = useMutation({
    mutationFn: async ({ emailId, category }: { emailId: number; category: string }) => {
      const response = await apiClient.emailUpdateCategory(emailId, category);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['emails'] });
      queryClient.invalidateQueries({ queryKey: ['emailStats'] });
    },
  });

  // Link customer mutation
  const linkCustomerMutation = useMutation({
    mutationFn: async ({ emailId, accountCode }: { emailId: number; accountCode: string }) => {
      const response = await apiClient.emailLinkCustomer(emailId, accountCode);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['emails'] });
      setLinkAccountInput('');
    },
  });

  // Unlink customer mutation
  const unlinkCustomerMutation = useMutation({
    mutationFn: async (emailId: number) => {
      const response = await apiClient.emailUnlinkCustomer(emailId);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['emails'] });
    },
  });

  // AI categorize mutation
  const categorizeMutation = useMutation({
    mutationFn: async (emailId: number) => {
      const response = await apiClient.emailCategorize(emailId);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['emails'] });
      queryClient.invalidateQueries({ queryKey: ['emailStats'] });
    },
  });

  const handleSearch = () => {
    setFilters((prev) => ({ ...prev, search: searchTerm, page: 1 }));
  };

  const handleCategoryFilter = (category: string | undefined) => {
    setFilters((prev) => ({ ...prev, category, page: 1 }));
  };

  const emails = emailsQuery.data?.emails || [];
  const stats = statsQuery.data?.stats;
  const categories = statsQuery.data?.categories || {};
  const providers = providersQuery.data?.providers || [];
  const syncStatus = syncStatusQuery.data;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Mail className="h-8 w-8 text-blue-600" />
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Email</h1>
            <p className="text-sm text-gray-600">
              {stats?.total_emails || 0} emails, {stats?.unread_count || 0} unread
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => syncMutation.mutate()}
            disabled={syncMutation.isPending}
            className="btn btn-primary flex items-center gap-2"
          >
            <RefreshCw className={`h-4 w-4 ${syncMutation.isPending ? 'animate-spin' : ''}`} />
            Sync
          </button>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <div
          className={`card cursor-pointer ${!filters.category ? 'ring-2 ring-blue-500' : ''}`}
          onClick={() => handleCategoryFilter(undefined)}
        >
          <div className="flex items-center gap-2">
            <Inbox className="h-5 w-5 text-gray-600" />
            <span className="text-sm text-gray-600">All</span>
          </div>
          <p className="text-2xl font-bold text-gray-900">{stats?.total_emails || 0}</p>
        </div>
        {CATEGORIES.slice(0, 4).map((cat) => (
          <div
            key={cat.value}
            className={`card cursor-pointer ${filters.category === cat.value ? 'ring-2 ring-blue-500' : ''}`}
            onClick={() => handleCategoryFilter(cat.value)}
          >
            <div className="flex items-center gap-2">
              <span className={`px-2 py-0.5 rounded text-xs ${cat.color}`}>{cat.label}</span>
            </div>
            <p className="text-2xl font-bold text-gray-900">{categories[cat.value] || 0}</p>
          </div>
        ))}
      </div>

      {/* Providers Status */}
      {providers.length === 0 && (
        <div className="card bg-yellow-50 border-yellow-200">
          <div className="flex items-center gap-3">
            <AlertCircle className="h-6 w-6 text-yellow-600" />
            <div>
              <p className="font-medium text-yellow-800">No email providers configured</p>
              <p className="text-sm text-yellow-700">
                Configure an email provider in config.ini to start syncing emails.
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Search and Filters */}
      <div className="card">
        <div className="flex flex-col md:flex-row gap-4">
          <div className="flex-1 flex gap-2">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
              <input
                type="text"
                placeholder="Search emails..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
                className="w-full pl-10 pr-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
            <button onClick={handleSearch} className="btn btn-secondary">
              Search
            </button>
          </div>
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`btn ${showFilters ? 'btn-primary' : 'btn-secondary'} flex items-center gap-2`}
          >
            <Filter className="h-4 w-4" />
            Filters
          </button>
        </div>

        {showFilters && (
          <div className="mt-4 pt-4 border-t grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Category</label>
              <select
                value={filters.category || ''}
                onChange={(e) => handleCategoryFilter(e.target.value || undefined)}
                className="w-full border rounded-lg px-3 py-2"
              >
                <option value="">All categories</option>
                {CATEGORIES.map((cat) => (
                  <option key={cat.value} value={cat.value}>
                    {cat.label}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Read Status</label>
              <select
                value={filters.is_read === undefined ? '' : filters.is_read ? 'read' : 'unread'}
                onChange={(e) =>
                  setFilters((prev) => ({
                    ...prev,
                    is_read: e.target.value === '' ? undefined : e.target.value === 'read',
                    page: 1,
                  }))
                }
                className="w-full border rounded-lg px-3 py-2"
              >
                <option value="">All</option>
                <option value="unread">Unread</option>
                <option value="read">Read</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Linked</label>
              <select
                value={filters.linked_account === undefined ? 'all' : filters.linked_account === '' ? 'unlinked' : 'linked'}
                onChange={(e) =>
                  setFilters((prev) => ({
                    ...prev,
                    linked_account: e.target.value === 'all' ? undefined : e.target.value === 'linked' ? 'any' : '',
                    page: 1,
                  }))
                }
                className="w-full border rounded-lg px-3 py-2"
              >
                <option value="all">All</option>
                <option value="linked">Linked</option>
                <option value="unlinked">Unlinked</option>
              </select>
            </div>
            <div className="flex items-end">
              <button
                onClick={() => {
                  setFilters({ page: 1, page_size: 50 });
                  setSearchTerm('');
                }}
                className="btn btn-secondary w-full"
              >
                Clear Filters
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Email List and Detail */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Email List */}
        <div className="lg:col-span-1 card p-0 overflow-hidden">
          <div className="max-h-[600px] overflow-y-auto">
            {emailsQuery.isLoading ? (
              <div className="p-4 text-center text-gray-500">Loading emails...</div>
            ) : emails.length === 0 ? (
              <div className="p-4 text-center text-gray-500">No emails found</div>
            ) : (
              <div className="divide-y">
                {emails.map((email) => (
                  <div
                    key={email.id}
                    className={`p-3 cursor-pointer hover:bg-gray-50 ${
                      selectedEmail?.id === email.id ? 'bg-blue-50' : ''
                    } ${!email.is_read ? 'bg-blue-50/30' : ''}`}
                    onClick={() => setSelectedEmail(email)}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="min-w-0 flex-1">
                        <div className="flex items-center gap-2">
                          <p className={`text-sm truncate ${!email.is_read ? 'font-semibold' : ''}`}>
                            {email.from_name || email.from_address}
                          </p>
                          {email.has_attachments && <Paperclip className="h-3 w-3 text-gray-400 flex-shrink-0" />}
                        </div>
                        <p className={`text-sm truncate ${!email.is_read ? 'font-medium text-gray-900' : 'text-gray-700'}`}>
                          {email.subject || '(No subject)'}
                        </p>
                        <p className="text-xs text-gray-500 truncate">{email.body_preview}</p>
                      </div>
                      <div className="flex flex-col items-end gap-1 flex-shrink-0">
                        <span className="text-xs text-gray-500">{formatDate(email.received_at)}</span>
                        {email.category && (
                          <span className={`px-1.5 py-0.5 rounded text-xs ${getCategoryStyle(email.category)}`}>
                            {getCategoryLabel(email.category)}
                          </span>
                        )}
                        {email.linked_account && (
                          <span className="flex items-center gap-1 text-xs text-blue-600">
                            <Link className="h-3 w-3" />
                            {email.linked_account}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Pagination */}
          {emailsQuery.data && emailsQuery.data.total_pages > 1 && (
            <div className="p-3 border-t flex items-center justify-between">
              <button
                onClick={() => setFilters((prev) => ({ ...prev, page: Math.max(1, (prev.page || 1) - 1) }))}
                disabled={filters.page === 1}
                className="btn btn-secondary btn-sm"
              >
                Previous
              </button>
              <span className="text-sm text-gray-600">
                Page {filters.page} of {emailsQuery.data.total_pages}
              </span>
              <button
                onClick={() =>
                  setFilters((prev) => ({
                    ...prev,
                    page: Math.min(emailsQuery.data?.total_pages || 1, (prev.page || 1) + 1),
                  }))
                }
                disabled={filters.page === emailsQuery.data.total_pages}
                className="btn btn-secondary btn-sm"
              >
                Next
              </button>
            </div>
          )}
        </div>

        {/* Email Detail */}
        <div className="lg:col-span-2 card">
          {selectedEmail ? (
            <div className="space-y-4">
              {/* Email Header */}
              <div className="flex items-start justify-between">
                <div className="min-w-0 flex-1">
                  <h2 className="text-lg font-semibold text-gray-900">{selectedEmail.subject || '(No subject)'}</h2>
                  <div className="mt-1 flex items-center gap-2 text-sm text-gray-600">
                    <User className="h-4 w-4" />
                    <span>
                      {selectedEmail.from_name ? `${selectedEmail.from_name} <${selectedEmail.from_address}>` : selectedEmail.from_address}
                    </span>
                  </div>
                  <div className="mt-1 flex items-center gap-2 text-sm text-gray-500">
                    <Clock className="h-4 w-4" />
                    <span>{new Date(selectedEmail.received_at).toLocaleString()}</span>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  {selectedEmail.category && (
                    <span className={`px-2 py-1 rounded text-sm ${getCategoryStyle(selectedEmail.category)}`}>
                      {getCategoryLabel(selectedEmail.category)}
                    </span>
                  )}
                </div>
              </div>

              {/* Category Actions */}
              <div className="flex flex-wrap items-center gap-2 p-3 bg-gray-50 rounded-lg">
                <span className="text-sm text-gray-600 mr-2">Category:</span>
                {CATEGORIES.filter((c) => c.value !== 'uncategorized').map((cat) => (
                  <button
                    key={cat.value}
                    onClick={() => updateCategoryMutation.mutate({ emailId: selectedEmail.id, category: cat.value })}
                    className={`px-2 py-1 rounded text-xs ${
                      selectedEmail.category === cat.value ? cat.color + ' ring-2 ring-offset-1 ring-gray-400' : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    {cat.label}
                  </button>
                ))}
                <button
                  onClick={() => categorizeMutation.mutate(selectedEmail.id)}
                  disabled={categorizeMutation.isPending}
                  className="ml-2 px-2 py-1 rounded text-xs bg-blue-100 text-blue-800 hover:bg-blue-200"
                >
                  {categorizeMutation.isPending ? 'Analyzing...' : 'AI Categorize'}
                </button>
              </div>

              {/* Customer Link */}
              <div className="flex flex-wrap items-center gap-2 p-3 bg-gray-50 rounded-lg">
                <span className="text-sm text-gray-600 mr-2">Customer:</span>
                {selectedEmail.linked_account ? (
                  <div className="flex items-center gap-2">
                    <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded text-sm">
                      {selectedEmail.linked_account}
                      {selectedEmail.linked_customer_name && ` - ${selectedEmail.linked_customer_name}`}
                    </span>
                    <button
                      onClick={() => unlinkCustomerMutation.mutate(selectedEmail.id)}
                      className="p-1 text-red-600 hover:bg-red-50 rounded"
                      title="Unlink customer"
                    >
                      <Unlink className="h-4 w-4" />
                    </button>
                  </div>
                ) : (
                  <div className="flex items-center gap-2 flex-wrap">
                    <input
                      type="text"
                      placeholder="Account code"
                      value={linkAccountInput}
                      onChange={(e) => setLinkAccountInput(e.target.value)}
                      className="px-2 py-1 border rounded text-sm w-32"
                    />
                    <button
                      onClick={() => {
                        linkCustomerMutation.mutate({ emailId: selectedEmail.id, accountCode: linkAccountInput });
                      }}
                      disabled={linkCustomerMutation.isPending}
                      className="px-2 py-1 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50"
                    >
                      {linkCustomerMutation.isPending ? 'Linking...' : 'Link'}
                    </button>
                    {linkCustomerMutation.isError && (
                      <span className="text-red-600 text-xs">Failed to link customer</span>
                    )}
                    {linkCustomerMutation.data && !linkCustomerMutation.data.success && (
                      <span className="text-red-600 text-xs">{(linkCustomerMutation.data as { success: boolean; error?: string }).error || 'Failed to link'}</span>
                    )}
                  </div>
                )}
              </div>

              {/* Email Body */}
              <div className="border-t pt-4">
                <div className="prose prose-sm max-w-none">
                  {selectedEmail.body_text ? (
                    <pre className="whitespace-pre-wrap font-sans text-sm text-gray-700">{selectedEmail.body_text}</pre>
                  ) : (
                    <p className="text-gray-500 italic">{selectedEmail.body_preview || 'No content'}</p>
                  )}
                </div>
              </div>

              {/* Attachments */}
              {selectedEmail.has_attachments && (
                <div className="border-t pt-4">
                  <h3 className="text-sm font-medium text-gray-700 mb-2 flex items-center gap-2">
                    <Paperclip className="h-4 w-4" />
                    Attachments
                  </h3>
                  <p className="text-sm text-gray-500">This email has attachments (download not implemented yet)</p>
                </div>
              )}
            </div>
          ) : (
            <div className="flex flex-col items-center justify-center h-64 text-gray-500">
              <Mail className="h-12 w-12 mb-2 opacity-50" />
              <p>Select an email to view details</p>
            </div>
          )}
        </div>
      </div>

      {/* Sync Status */}
      {syncStatus && (
        <div className="card">
          <h3 className="text-lg font-semibold text-gray-900 mb-3">Sync Status</h3>
          <div className="space-y-2">
            {syncStatus.providers.map((provider) => (
              <div key={provider.id} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                <div className="flex items-center gap-3">
                  {provider.sync_status === 'success' && <CheckCircle className="h-5 w-5 text-green-500" />}
                  {provider.sync_status === 'failed' && <XCircle className="h-5 w-5 text-red-500" />}
                  {provider.sync_status === 'running' && <RefreshCw className="h-5 w-5 text-blue-500 animate-spin" />}
                  {provider.sync_status === 'pending' && <Clock className="h-5 w-5 text-gray-400" />}
                  <div>
                    <p className="font-medium">{provider.name}</p>
                    <p className="text-xs text-gray-500">{provider.type}</p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="text-sm">{provider.last_sync ? `Last sync: ${formatDate(provider.last_sync)}` : 'Never synced'}</p>
                  <p className={`text-xs ${provider.enabled ? 'text-green-600' : 'text-gray-400'}`}>
                    {provider.enabled ? 'Enabled' : 'Disabled'}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default Email;
