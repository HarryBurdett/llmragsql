import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Building2, ChevronDown, Check, RefreshCw } from 'lucide-react';
import apiClient from '../api/client';
import type { Company } from '../api/client';

export function CompanySelector() {
  const [isOpen, setIsOpen] = useState(false);
  const queryClient = useQueryClient();

  const { data: companiesData, isLoading } = useQuery({
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
      setIsOpen(false);
    },
  });

  const companies = companiesData?.companies || [];
  const currentCompany = companiesData?.current_company;

  const handleSelect = (company: Company) => {
    if (company.id !== currentCompany?.id) {
      switchMutation.mutate(company.id);
    } else {
      setIsOpen(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center gap-2 px-3 py-2 text-sm text-gray-500">
        <RefreshCw className="h-4 w-4 animate-spin" />
        Loading...
      </div>
    );
  }

  return (
    <div className="relative">
      <button
        onClick={() => setIsOpen(!isOpen)}
        disabled={switchMutation.isPending}
        className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50"
      >
        {switchMutation.isPending ? (
          <RefreshCw className="h-4 w-4 animate-spin text-blue-600" />
        ) : (
          <Building2 className="h-4 w-4 text-blue-600" />
        )}
        <span className="max-w-[150px] truncate">
          {currentCompany?.name || 'Select Company'}
        </span>
        <ChevronDown className={`h-4 w-4 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
      </button>

      {isOpen && (
        <>
          <div
            className="fixed inset-0 z-10"
            onClick={() => setIsOpen(false)}
          />
          <div className="absolute right-0 mt-2 w-64 bg-white border border-gray-200 rounded-lg shadow-lg z-20">
            <div className="p-2 border-b border-gray-100">
              <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">
                Switch Company
              </p>
            </div>
            <div className="py-1">
              {companies.map((company) => (
                <button
                  key={company.id}
                  onClick={() => handleSelect(company)}
                  className={`w-full flex items-center justify-between px-4 py-3 text-sm hover:bg-gray-50 ${
                    company.id === currentCompany?.id ? 'bg-blue-50' : ''
                  }`}
                >
                  <div className="flex flex-col items-start">
                    <span className={`font-medium ${
                      company.id === currentCompany?.id ? 'text-blue-700' : 'text-gray-900'
                    }`}>
                      {company.name}
                    </span>
                    <span className="text-xs text-gray-500">{company.description}</span>
                  </div>
                  {company.id === currentCompany?.id && (
                    <Check className="h-4 w-4 text-blue-600" />
                  )}
                </button>
              ))}
            </div>
            {companies.length === 0 && (
              <div className="px-4 py-3 text-sm text-gray-500">
                No companies configured
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}

export default CompanySelector;
