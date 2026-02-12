import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Building2, Check, RefreshCw, Search, AlertCircle } from 'lucide-react';
import apiClient, { authFetch } from '../api/client';
import type { Company as CompanyType } from '../api/client';

export function Company() {
  const queryClient = useQueryClient();
  const [discoverMessage, setDiscoverMessage] = useState<string | null>(null);
  const [discoverError, setDiscoverError] = useState<string | null>(null);

  const { data: companiesData, isLoading, refetch } = useQuery({
    queryKey: ['companies'],
    queryFn: async () => {
      const response = await apiClient.getCompanies();
      return response.data;
    },
  });

  const switchMutation = useMutation({
    mutationFn: async (companyId: string) => {
      const response = await apiClient.switchCompany(companyId);
      return response.data;
    },
    onSuccess: () => {
      // Invalidate all queries to refresh data from new database
      queryClient.invalidateQueries();
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

  const companies = companiesData?.companies || [];
  const currentCompany = companiesData?.current_company;

  const handleSelect = (company: CompanyType) => {
    if (company.id !== currentCompany?.id) {
      switchMutation.mutate(company.id);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <RefreshCw className="h-8 w-8 animate-spin text-blue-600" />
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Company Selection</h2>
          <p className="text-gray-600 mt-1">Select the company database to work with</p>
        </div>
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
      </div>

      {discoverMessage && (
        <div className="p-3 bg-green-50 text-green-700 rounded-lg flex items-center gap-2">
          <Check className="h-4 w-4" />
          {discoverMessage}
        </div>
      )}

      {discoverError && (
        <div className="p-3 bg-red-50 text-red-700 rounded-lg flex items-center gap-2">
          <AlertCircle className="h-4 w-4" />
          {discoverError}
        </div>
      )}

      <div className="card">
        <div className="flex items-center gap-2 mb-4">
          <Building2 className="h-5 w-5 text-blue-600" />
          <h3 className="text-lg font-semibold">Available Companies</h3>
        </div>

        {switchMutation.isPending && (
          <div className="mb-4 p-3 bg-blue-50 text-blue-700 rounded-lg flex items-center gap-2">
            <RefreshCw className="h-4 w-4 animate-spin" />
            Switching company...
          </div>
        )}

        <div className="grid gap-3">
          {companies.map((company) => (
            <button
              key={company.id}
              onClick={() => handleSelect(company)}
              disabled={switchMutation.isPending}
              className={`w-full flex items-center justify-between p-4 rounded-lg border-2 transition-colors text-left ${
                company.id === currentCompany?.id
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 hover:border-gray-300 hover:bg-gray-50'
              } ${switchMutation.isPending ? 'opacity-50 cursor-not-allowed' : ''}`}
            >
              <div className="flex items-center gap-3">
                <Building2 className={`h-6 w-6 ${
                  company.id === currentCompany?.id ? 'text-blue-600' : 'text-gray-400'
                }`} />
                <div>
                  <p className={`font-medium ${
                    company.id === currentCompany?.id ? 'text-blue-700' : 'text-gray-900'
                  }`}>
                    {company.name}
                  </p>
                  <p className="text-sm text-gray-500">{company.description}</p>
                </div>
              </div>
              {company.id === currentCompany?.id && (
                <div className="flex items-center gap-2">
                  <span className="text-sm text-blue-600 font-medium">Current</span>
                  <Check className="h-5 w-5 text-blue-600" />
                </div>
              )}
            </button>
          ))}
        </div>

        {companies.length === 0 && (
          <div className="text-center py-8 text-gray-500">
            <Building2 className="h-12 w-12 mx-auto mb-3 text-gray-300" />
            <p>No companies configured</p>
            <p className="text-sm mt-1">Click "Discover Companies" to auto-detect Opera installations</p>
          </div>
        )}
      </div>
    </div>
  );
}
