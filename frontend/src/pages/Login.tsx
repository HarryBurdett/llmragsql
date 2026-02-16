import { useState, useEffect, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { useAuth } from '../context/AuthContext';
import { Building2, User, Lock, Briefcase } from 'lucide-react';
import apiClient from '../api/client';

interface Company {
  id: string;
  name: string;
  description?: string;
}

interface License {
  id: number;
  client_name: string;
  opera_version: string;
  max_users: number;
  is_active: boolean;
}

export function Login() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  // License/Client selection state
  const [licenses, setLicenses] = useState<License[]>([]);
  const [selectedLicense, setSelectedLicense] = useState<number | null>(null);
  const [licensesLoading, setLicensesLoading] = useState(true);

  // Company selection state
  const [companies, setCompanies] = useState<Company[]>([]);
  const [selectedCompany, setSelectedCompany] = useState<string | null>(null);
  const [companiesLoading, setCompaniesLoading] = useState(true);
  const [userDefaultCompany, setUserDefaultCompany] = useState<string | null>(null);

  const { login } = useAuth();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  // Fetch licenses and companies on mount
  useEffect(() => {
    async function fetchLicenses() {
      try {
        const response = await fetch('http://localhost:8000/api/licenses');
        const data = await response.json();
        setLicenses(data.licenses || []);
        // Select first license by default if available
        if (data.licenses?.length > 0) {
          setSelectedLicense(data.licenses[0].id);
        }
      } catch (err) {
        console.error('Failed to fetch licenses:', err);
      } finally {
        setLicensesLoading(false);
      }
    }

    async function fetchCompanies() {
      try {
        const response = await fetch('http://localhost:8000/api/companies/list');
        const data = await response.json();
        setCompanies(data.companies || []);
        // Select first company by default if available
        if (data.companies?.length > 0) {
          setSelectedCompany(data.companies[0].id);
        }
      } catch (err) {
        console.error('Failed to fetch companies:', err);
      } finally {
        setCompaniesLoading(false);
      }
    }

    fetchLicenses();
    fetchCompanies();
  }, []);

  // Update selected company when user's default is known (after username blur)
  const handleUsernameBlur = async () => {
    if (!username.trim()) return;

    try {
      // Try to get the user's default company
      const response = await fetch(`http://localhost:8000/api/auth/user-default-company?username=${encodeURIComponent(username)}`);
      if (response.ok) {
        const data = await response.json();
        if (data.default_company) {
          setUserDefaultCompany(data.default_company);
          setSelectedCompany(data.default_company);
        }
      }
    } catch (err) {
      // Ignore errors - this is just a convenience feature
    }
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);

    // Validate license selection (only if there are multiple licenses)
    if (licenses.length > 1 && !selectedLicense) {
      setError('Please select a client');
      return;
    }

    // Validate company selection
    if (!selectedCompany) {
      setError('Please select a company');
      return;
    }

    setIsLoading(true);

    try {
      const result = await login(username, password);

      if (result.success) {
        // Clear any cached queries
        queryClient.clear();

        // Switch to the selected company
        await apiClient.switchCompany(selectedCompany);

        // Navigate to home
        navigate('/', { replace: true });
      } else {
        setError(result.error || 'Invalid username or password');
      }
    } catch (err) {
      setError('An error occurred. Please try again.');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 flex flex-col items-center justify-center px-4">
      {/* Crakd.AI Logo */}
      <div className="mb-8 flex items-center gap-3">
        <div
          className="w-12 h-12 rounded-xl flex items-center justify-center font-extrabold text-xl text-white"
          style={{ background: 'linear-gradient(135deg, #3b82f6, #8b5cf6)' }}
        >
          C
        </div>
        <div className="text-2xl font-bold text-white">
          Crakd<span className="text-blue-400">.ai</span>
        </div>
      </div>

      {/* Login Card */}
      <div className="w-full max-w-md bg-white rounded-2xl shadow-2xl p-8">
        <h2 className="text-2xl font-bold text-center text-gray-900 mb-2">
          SQL RAG Login
        </h2>
        <p className="text-center text-gray-600 mb-8">
          Enter your credentials to continue
        </p>

        <form onSubmit={handleSubmit} className="space-y-5">
          {/* Client Selection - only show if multiple licenses */}
          {licenses.length > 1 && (
            <div>
              <label htmlFor="client" className="block text-sm font-medium text-gray-700 mb-1">
                Client
              </label>
              <div className="relative">
                <Briefcase className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400 pointer-events-none" />
                <select
                  id="client"
                  value={selectedLicense || ''}
                  onChange={(e) => setSelectedLicense(e.target.value ? Number(e.target.value) : null)}
                  className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all outline-none bg-white cursor-pointer"
                  disabled={licensesLoading}
                  tabIndex={1}
                  style={{ WebkitAppearance: 'menulist', MozAppearance: 'menulist' }}
                >
                  <option value="">Select client...</option>
                  {licenses.map((license) => (
                    <option key={license.id} value={license.id}>
                      {license.client_name}
                    </option>
                  ))}
                </select>
              </div>
            </div>
          )}

          {/* Username */}
          <div>
            <label htmlFor="username" className="block text-sm font-medium text-gray-700 mb-1">
              Username
            </label>
            <div className="relative">
              <User className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400" />
              <input
                id="username"
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                onBlur={handleUsernameBlur}
                required
                autoComplete="username"
                autoFocus
                tabIndex={2}
                className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all outline-none"
                placeholder="Enter your username"
              />
            </div>
          </div>

          {/* Password */}
          <div>
            <label htmlFor="password" className="block text-sm font-medium text-gray-700 mb-1">
              Password
            </label>
            <div className="relative">
              <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400" />
              <input
                id="password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                autoComplete="current-password"
                tabIndex={3}
                className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all outline-none"
                placeholder="Enter your password"
              />
            </div>
          </div>

          {/* Company Selection */}
          <div>
            <label htmlFor="company" className="block text-sm font-medium text-gray-700 mb-1">
              Company
            </label>
            <div className="relative">
              <Building2 className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400 pointer-events-none" />
              <select
                id="company"
                value={selectedCompany || ''}
                onChange={(e) => setSelectedCompany(e.target.value || null)}
                className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all outline-none bg-white cursor-pointer"
                disabled={companiesLoading}
                tabIndex={4}
                style={{ WebkitAppearance: 'menulist', MozAppearance: 'menulist' }}
              >
                <option value="">Select company...</option>
                {companies.map((company) => (
                  <option key={company.id} value={company.id}>
                    {company.name}
                    {company.id === userDefaultCompany ? ' (Default)' : ''}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {error && (
            <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
              <span className="text-red-500 text-lg">!</span>
              <p className="text-sm text-red-700">{error}</p>
            </div>
          )}

          <button
            type="submit"
            disabled={isLoading || companiesLoading || licensesLoading}
            tabIndex={5}
            className="w-full py-3 px-4 bg-gradient-to-r from-blue-500 to-purple-600 text-white font-semibold rounded-lg shadow-lg hover:from-blue-600 hover:to-purple-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
          >
            {isLoading ? (
              <>
                <svg className="animate-spin h-5 w-5" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                Signing in...
              </>
            ) : (
              'Sign In'
            )}
          </button>
        </form>
      </div>

      {/* Footer */}
      <p className="mt-8 text-gray-500 text-sm">
        Powered by{' '}
        <a
          href="https://crakd.ai"
          target="_blank"
          rel="noopener noreferrer"
          className="text-blue-400 hover:text-blue-300 transition-colors"
        >
          Crakd.ai
        </a>
      </p>
    </div>
  );
}
