import { useEffect, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Building2, AlertCircle, RefreshCw } from 'lucide-react';
import apiClient from '../api/client';
import type { Company } from '../api/client';
import { useAuth } from '../context/AuthContext';

interface CompanyRequiredModalProps {
  children: React.ReactNode;
}

export function CompanyRequiredModal({ children }: CompanyRequiredModalProps) {
  const queryClient = useQueryClient();
  const { user, isAuthenticated, isLoading: authLoading } = useAuth();
  const autoSwitchAttempted = useRef(false);

  const { data: companiesData, isLoading, error } = useQuery({
    queryKey: ['companies'],
    queryFn: async () => {
      const response = await apiClient.getCompanies();
      return response.data;
    },
    // Only fetch companies when authenticated
    enabled: isAuthenticated && !authLoading,
  });

  const switchMutation = useMutation({
    mutationFn: async (companyId: string) => {
      const response = await apiClient.switchCompany(companyId);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries();
    },
  });

  const companies = companiesData?.companies || [];
  const currentCompany = companiesData?.current_company;

  // Auto-switch to default company if set and no company currently selected
  useEffect(() => {
    if (
      !authLoading &&
      !isLoading &&
      !currentCompany &&
      user?.default_company &&
      companies.length > 0 &&
      !autoSwitchAttempted.current &&
      !switchMutation.isPending
    ) {
      // Check if default company exists in the list
      const defaultCompanyExists = companies.some(
        (c: Company) => c.id === user.default_company
      );
      if (defaultCompanyExists) {
        autoSwitchAttempted.current = true;
        switchMutation.mutate(user.default_company);
      }
    }
  }, [authLoading, isLoading, currentCompany, user, companies, switchMutation]);

  // Reset auto-switch flag when user changes
  useEffect(() => {
    autoSwitchAttempted.current = false;
  }, [user?.id]);

  // If auth is loading or companies are loading, show loading state
  if (authLoading || isLoading || switchMutation.isPending) {
    return (
      <div className="fixed inset-0 bg-gray-900 bg-opacity-50 flex items-center justify-center z-50">
        <div className="bg-white rounded-lg p-8 max-w-md w-full mx-4 shadow-xl">
          <div className="flex flex-col items-center">
            <RefreshCw className="h-12 w-12 text-blue-600 animate-spin mb-4" />
            <h2 className="text-xl font-semibold text-gray-900">Loading...</h2>
            <p className="text-gray-500 mt-2">
              {switchMutation.isPending ? 'Switching company...' : 'Checking company configuration'}
            </p>
          </div>
        </div>
      </div>
    );
  }

  // If error, show error state
  if (error) {
    return (
      <div className="fixed inset-0 bg-gray-900 bg-opacity-50 flex items-center justify-center z-50">
        <div className="bg-white rounded-lg p-8 max-w-md w-full mx-4 shadow-xl">
          <div className="flex flex-col items-center">
            <AlertCircle className="h-12 w-12 text-red-600 mb-4" />
            <h2 className="text-xl font-semibold text-gray-900">Connection Error</h2>
            <p className="text-gray-500 mt-2 text-center">
              Unable to connect to the server. Please check your connection and refresh.
            </p>
            <button
              onClick={() => window.location.reload()}
              className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
            >
              Refresh Page
            </button>
          </div>
        </div>
      </div>
    );
  }

  // If no company selected, show company selection modal
  if (!currentCompany) {
    return (
      <div className="fixed inset-0 bg-gray-900 bg-opacity-50 flex items-center justify-center z-50">
        <div className="bg-white rounded-lg p-8 max-w-md w-full mx-4 shadow-xl">
          <div className="flex flex-col items-center mb-6">
            <Building2 className="h-12 w-12 text-blue-600 mb-4" />
            <h2 className="text-xl font-semibold text-gray-900">Select Company</h2>
            <p className="text-gray-500 mt-2 text-center">
              Please select a company to continue
            </p>
          </div>

          <div className="space-y-2">
            {companies.map((company: Company) => (
              <button
                key={company.id}
                onClick={() => switchMutation.mutate(company.id)}
                disabled={switchMutation.isPending}
                className="w-full flex items-center justify-between p-4 border border-gray-200 rounded-lg hover:bg-blue-50 hover:border-blue-300 transition-colors disabled:opacity-50"
              >
                <div className="flex flex-col items-start">
                  <span className="font-medium text-gray-900">{company.name}</span>
                  <span className="text-sm text-gray-500">{company.description}</span>
                </div>
                {switchMutation.isPending && switchMutation.variables === company.id && (
                  <RefreshCw className="h-5 w-5 text-blue-600 animate-spin" />
                )}
              </button>
            ))}
          </div>

          {companies.length === 0 && (
            <div className="text-center py-8">
              <AlertCircle className="h-8 w-8 text-amber-500 mx-auto mb-2" />
              <p className="text-gray-500">No companies configured</p>
              <p className="text-sm text-gray-400 mt-1">
                Please configure companies in settings
              </p>
            </div>
          )}
        </div>
      </div>
    );
  }

  // Company is selected, render children
  return <>{children}</>;
}

export default CompanyRequiredModal;
