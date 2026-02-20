import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Building2, Check, RefreshCw, Search } from 'lucide-react';
import apiClient, { authFetch } from '../api/client';
import { useAuth } from '../context/AuthContext';
import { PageHeader, Card, Alert, LoadingState, EmptyState, StatusBadge } from '../components/ui';

export function Company() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const [discoverMessage, setDiscoverMessage] = useState<string | null>(null);
  const [discoverError, setDiscoverError] = useState<string | null>(null);
  const [switchMessage, setSwitchMessage] = useState<string | null>(null);

  const { data: companiesData, isLoading, refetch } = useQuery({
    queryKey: ['companies'],
    queryFn: async () => {
      const response = await apiClient.getCompanies();
      return response.data;
    },
  });

  const discoverMutation = useMutation({
    mutationFn: async () => {
      const response = await authFetch('/api/companies/discover', { method: 'POST' });
      return response.json();
    },
    onSuccess: (data) => {
      if (data.created && data.created.length > 0) {
        setDiscoverMessage(`Discovered ${data.created.length} new companies: ${data.created.join(', ')}`);
        refetch(); // Refresh company list
      } else {
        setDiscoverMessage(data.message || 'No new companies found');
      }
      if (data.errors && data.errors.length > 0) {
        setDiscoverError(data.errors.join('; '));
      } else {
        setDiscoverError(null);
      }
      setTimeout(() => setDiscoverMessage(null), 10000);
    },
    onError: (error: Error) => {
      setDiscoverError(error.message);
      setTimeout(() => setDiscoverError(null), 10000);
    }
  });

  const switchMutation = useMutation({
    mutationFn: async (companyId: string) => {
      const response = await apiClient.switchCompany(companyId);
      return response.data;
    },
    onSuccess: (data) => {
      setSwitchMessage(`Switched to ${data.company?.name || 'company'}`);
      // Invalidate all queries to refresh data for new company
      queryClient.invalidateQueries();
      setTimeout(() => setSwitchMessage(null), 5000);
    },
    onError: (error: Error) => {
      setDiscoverError(error.message);
      setTimeout(() => setDiscoverError(null), 10000);
    }
  });

  const companies = companiesData?.companies || [];
  const currentCompany = companiesData?.current_company;

  const handleSwitchCompany = (companyId: string) => {
    if (companyId !== currentCompany?.id) {
      switchMutation.mutate(companyId);
    }
  };

  if (isLoading) {
    return <LoadingState message="Loading companies..." />;
  }

  return (
    <div className="space-y-6">
      <PageHeader icon={Building2} title="Company" subtitle="Current company and available databases">
        {user?.is_admin && (
          <button
            onClick={() => discoverMutation.mutate()}
            disabled={discoverMutation.isPending}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            {discoverMutation.isPending ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <Search className="h-4 w-4" />
            )}
            Discover Companies
          </button>
        )}
      </PageHeader>

      {discoverMessage && (
        <Alert variant="success" onDismiss={() => setDiscoverMessage(null)}>
          {discoverMessage}
        </Alert>
      )}

      {switchMessage && (
        <Alert variant="info" onDismiss={() => setSwitchMessage(null)}>
          {switchMessage}
        </Alert>
      )}

      {discoverError && (
        <Alert variant="error" onDismiss={() => setDiscoverError(null)}>
          {discoverError}
        </Alert>
      )}

      {/* Current Company */}
      <Card title="Current Company" icon={Building2}>
        {currentCompany ? (
          <div className="flex items-center justify-between p-4 rounded-lg border-2 border-blue-500 bg-blue-50">
            <div className="flex items-center gap-3">
              <Building2 className="h-8 w-8 text-blue-600" />
              <div>
                <p className="font-semibold text-blue-700 text-base">
                  {currentCompany.name}
                </p>
                <p className="text-sm text-gray-600">{currentCompany.description}</p>
              </div>
            </div>
            <StatusBadge variant="success">Active</StatusBadge>
          </div>
        ) : (
          <EmptyState icon={Building2} title="No company selected" message="Select a company from the list below." />
        )}
      </Card>

      {/* Available Companies - click to switch */}
      <Card>
        <div className="flex items-center gap-2 mb-4">
          <Building2 className="h-5 w-5 text-gray-600" />
          <h3 className="text-base font-semibold text-gray-900">Available Companies</h3>
          <span className="text-xs text-gray-500">(click to switch)</span>
        </div>

        <div className="grid gap-3">
          {companies.map((company) => (
            <button
              key={company.id}
              onClick={() => handleSwitchCompany(company.id)}
              disabled={switchMutation.isPending}
              className={`flex items-center justify-between p-4 rounded-lg border transition-all w-full text-left ${
                company.id === currentCompany?.id
                  ? 'border-blue-300 bg-blue-50/50 cursor-default'
                  : 'border-gray-200 bg-white hover:border-blue-200 hover:bg-blue-50/30 cursor-pointer'
              } ${switchMutation.isPending ? 'opacity-50' : ''}`}
            >
              <div className="flex items-center gap-3">
                <Building2 className={`h-5 w-5 ${
                  company.id === currentCompany?.id ? 'text-blue-600' : 'text-gray-400'
                }`} />
                <div>
                  <p className={`text-sm font-medium ${
                    company.id === currentCompany?.id ? 'text-blue-700' : 'text-gray-900'
                  }`}>
                    {company.name}
                  </p>
                  <p className="text-xs text-gray-500">{company.description}</p>
                </div>
              </div>
              {company.id === currentCompany?.id ? (
                <Check className="h-5 w-5 text-blue-600" />
              ) : (
                <span className="text-xs text-gray-400">Click to switch</span>
              )}
            </button>
          ))}
        </div>

        {companies.length === 0 && (
          <EmptyState
            icon={Building2}
            title="No companies configured"
            message={user?.is_admin ? 'Click "Discover Companies" to auto-detect Opera installations' : undefined}
          />
        )}
      </Card>
    </div>
  );
}
