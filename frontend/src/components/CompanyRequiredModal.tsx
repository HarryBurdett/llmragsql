import { useEffect, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { AlertCircle, RefreshCw } from 'lucide-react';
import apiClient from '../api/client';
import { useAuth } from '../context/AuthContext';

interface CompanyRequiredModalProps {
  children: React.ReactNode;
}

export function CompanyRequiredModal({ children }: CompanyRequiredModalProps) {
  const { user, isAuthenticated, isLoading: authLoading } = useAuth();
  const lastUserId = useRef<number | null>(null);

  const { data: companiesData, isLoading, error } = useQuery({
    queryKey: ['companies'],
    queryFn: async () => {
      const response = await apiClient.getCompanies();
      return response.data;
    },
    // Only fetch companies when authenticated
    enabled: isAuthenticated && !authLoading,
  });

  const currentCompany = companiesData?.current_company;

  // Company selection is now handled in the Login page's second step
  // The auto-switch logic is no longer needed here since users explicitly
  // select their company during login

  // Track user ID changes to update reference
  useEffect(() => {
    if (user?.id) {
      lastUserId.current = user.id;
    }
  }, [user?.id]);

  // If auth is loading or companies are loading, show loading state
  if (authLoading || isLoading) {
    return (
      <div className="fixed inset-0 bg-gray-900 bg-opacity-50 flex items-center justify-center z-50">
        <div className="bg-white rounded-lg p-8 max-w-md w-full mx-4 shadow-xl">
          <div className="flex flex-col items-center">
            <RefreshCw className="h-12 w-12 text-blue-600 animate-spin mb-4" />
            <h2 className="text-xl font-semibold text-gray-900">Loading...</h2>
            <p className="text-gray-500 mt-2">
              Checking company configuration
            </p>
          </div>
        </div>
      </div>
    );
  }

  // If error, show error state
  if (error) {
    const errorMessage = (error as { response?: { status?: number; data?: { error?: string } }; message?: string })?.response?.data?.error
      || (error as { message?: string })?.message
      || 'Unknown error';
    const errorStatus = (error as { response?: { status?: number } })?.response?.status;

    return (
      <div className="fixed inset-0 bg-gray-900 bg-opacity-50 flex items-center justify-center z-50">
        <div className="bg-white rounded-lg p-8 max-w-md w-full mx-4 shadow-xl">
          <div className="flex flex-col items-center">
            <AlertCircle className="h-12 w-12 text-red-600 mb-4" />
            <h2 className="text-xl font-semibold text-gray-900">Connection Error</h2>
            <p className="text-gray-500 mt-2 text-center">
              Unable to connect to the server. Please check your connection and refresh.
            </p>
            <p className="text-xs text-gray-400 mt-2 text-center">
              {errorStatus ? `Status: ${errorStatus} - ` : ''}{errorMessage}
            </p>
            <div className="flex gap-2 mt-4">
              <button
                onClick={() => {
                  localStorage.clear();
                  window.location.href = '/login';
                }}
                className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300"
              >
                Clear & Login
              </button>
              <button
                onClick={() => window.location.reload()}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
              >
                Refresh Page
              </button>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // If no company selected, redirect to login to select one
  if (!currentCompany) {
    return (
      <div className="fixed inset-0 bg-gray-900 bg-opacity-50 flex items-center justify-center z-50">
        <div className="bg-white rounded-lg p-8 max-w-md w-full mx-4 shadow-xl">
          <div className="flex flex-col items-center">
            <AlertCircle className="h-12 w-12 text-amber-500 mb-4" />
            <h2 className="text-xl font-semibold text-gray-900">No Company Selected</h2>
            <p className="text-gray-500 mt-2 text-center">
              Please log in again to select a company.
            </p>
            <button
              onClick={() => {
                localStorage.clear();
                window.location.href = '/login';
              }}
              className="mt-4 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
            >
              Go to Login
            </button>
          </div>
        </div>
      </div>
    );
  }

  // Company is selected, render children
  return <>{children}</>;
}

export default CompanyRequiredModal;
