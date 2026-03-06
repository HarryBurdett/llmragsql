import { useState, useEffect, type FormEvent } from 'react';
import { useNavigate } from 'react-router-dom';
import { useQueryClient } from '@tanstack/react-query';
import { useAuth } from '../context/AuthContext';
import { Building2, User, Lock, Briefcase, ChevronDown, LogIn, AlertCircle, X } from 'lucide-react';
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

/** Turn raw backend errors into user-friendly messages */
function friendlyLoginError(raw: string): string {
  const lower = raw.toLowerCase();
  if (lower.includes('invalid username or password') || lower.includes('login failed'))
    return 'The username or password you entered is incorrect. Please check and try again.';
  if (lower.includes('not active') || lower.includes('disabled') || lower.includes('state'))
    return 'This account is not active. Please contact your administrator.';
  if (lower.includes('license') || lower.includes('max_users') || lower.includes('seat'))
    return 'All license seats are in use. Please try again later or ask an administrator to free a seat.';
  if (lower.includes('network') || lower.includes('fetch') || lower.includes('econnrefused'))
    return 'Cannot reach the server. Please check your network connection and try again.';
  if (lower.includes('timeout'))
    return 'The server took too long to respond. Please try again in a moment.';
  return raw;
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

  // Fetch user's default company when username changes (debounced)
  // Decoupled from blur/focus to avoid interfering with Tab key navigation
  useEffect(() => {
    if (!username.trim()) return;
    const timer = setTimeout(() => {
      fetch(`http://localhost:8000/api/auth/user-default-company?username=${encodeURIComponent(username)}`)
        .then(response => response.ok ? response.json() : null)
        .then(data => {
          if (data?.default_company) {
            setUserDefaultCompany(data.default_company);
            setSelectedCompany(data.default_company);
          }
        })
        .catch(() => {});
    }, 500);
    return () => clearTimeout(timer);
  }, [username]);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);

    // Validate license selection (only if there are multiple licenses)
    if (licenses.length > 1 && !selectedLicense) {
      setError('Please select a client to continue.');
      return;
    }

    // Validate company selection
    if (!selectedCompany) {
      setError('Please select a company to sign in to.');
      return;
    }

    setIsLoading(true);

    try {
      const result = await login(username, password);

      if (result.success) {
        // Switch to the selected company first, before clearing cache
        await apiClient.switchCompany(selectedCompany);

        // Now clear cached queries so CompanyRequiredModal re-fetches fresh data
        queryClient.clear();

        // Navigate to home
        navigate('/', { replace: true });
      } else {
        setError(friendlyLoginError(result.error || 'Login failed'));
      }
    } catch (err: any) {
      setError(friendlyLoginError(err?.message || 'Cannot reach the server. Please check your connection and try again.'));
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900 flex flex-col items-center justify-center px-4">
      {/* Login Card */}
      <div className="w-full max-w-md bg-white rounded-2xl shadow-2xl border border-gray-200/50 overflow-hidden">
        {/* Card Header with Crakd Logo */}
        <div className="bg-gradient-to-r from-blue-50 to-purple-50 border-b border-gray-100 px-8 pt-8 pb-6">
          <div className="flex items-center justify-center gap-2.5 mb-5">
            <div
              className="w-10 h-10 rounded-xl flex items-center justify-center font-extrabold text-lg text-white shadow-md"
              style={{ background: 'linear-gradient(135deg, #3b82f6, #8b5cf6)' }}
            >
              C
            </div>
            <div className="text-xl font-bold text-gray-800">
              Crakd<span className="text-blue-500">.ai</span>
            </div>
          </div>
          <div className="flex items-center justify-center gap-2 mb-1.5">
            <LogIn className="h-5 w-5 text-blue-600" />
            <h2 className="text-xl font-bold text-gray-900">
              Welcome
            </h2>
          </div>
          <p className="text-center text-sm text-gray-500">
            Sign in with your Opera credentials
          </p>
        </div>

        <div className="px-8 py-6">
          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Client Selection - only show if multiple licenses */}
            {licenses.length > 1 && (
              <div>
                <label htmlFor="client" className="block text-sm font-medium text-gray-700 mb-1.5">
                  Client
                </label>
                <div className="relative">
                  <Briefcase className="absolute left-3 top-1/2 -translate-y-1/2 h-4.5 w-4.5 text-gray-400 pointer-events-none" tabIndex={-1} aria-hidden="true" focusable="false" />
                  <select
                    id="client"
                    value={selectedLicense || ''}
                    onChange={(e) => setSelectedLicense(e.target.value ? Number(e.target.value) : null)}
                    className="w-full pl-10 pr-10 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all outline-none bg-white cursor-pointer appearance-none text-sm"
                    disabled={licensesLoading}
                  >
                    <option value="">Select client...</option>
                    {licenses.map((license) => (
                      <option key={license.id} value={license.id}>
                        {license.client_name}
                      </option>
                    ))}
                  </select>
                  <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" tabIndex={-1} aria-hidden="true" focusable="false" />
                </div>
              </div>
            )}

            {/* Username */}
            <div>
              <label htmlFor="username" className="block text-sm font-medium text-gray-700 mb-1.5">
                Username
              </label>
              <div className="relative">
                <User className="absolute left-3 top-1/2 -translate-y-1/2 h-4.5 w-4.5 text-gray-400 pointer-events-none" tabIndex={-1} aria-hidden="true" focusable="false" />
                <input
                  id="username"
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  required
                  autoComplete="username"
                  autoFocus
                  className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all outline-none text-sm"
                  placeholder="Your Opera username"
                />
              </div>
            </div>

            {/* Password */}
            <div>
              <label htmlFor="password" className="block text-sm font-medium text-gray-700 mb-1.5">
                Password
              </label>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-4.5 w-4.5 text-gray-400 pointer-events-none" tabIndex={-1} aria-hidden="true" focusable="false" />
                <input
                  id="password"
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                  autoComplete="current-password"
                  className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all outline-none text-sm"
                  placeholder="Your password"
                />
              </div>
            </div>

            {/* Company Selection */}
            <div>
              <label htmlFor="company" className="block text-sm font-medium text-gray-700 mb-1.5">
                Company
              </label>
              <div className="relative">
                <Building2 className="absolute left-3 top-1/2 -translate-y-1/2 h-4.5 w-4.5 text-gray-400 pointer-events-none" tabIndex={-1} aria-hidden="true" focusable="false" />
                <select
                  id="company"
                  value={selectedCompany || ''}
                  onChange={(e) => setSelectedCompany(e.target.value || null)}
                  className="w-full pl-10 pr-10 py-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all outline-none bg-white cursor-pointer appearance-none text-sm"
                  disabled={companiesLoading}
                >
                  <option value="">Select company...</option>
                  {companies.map((company) => (
                    <option key={company.id} value={company.id}>
                      {company.name}
                      {company.id === userDefaultCompany ? ' (Default)' : ''}
                    </option>
                  ))}
                </select>
                <ChevronDown className="absolute right-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400 pointer-events-none" tabIndex={-1} aria-hidden="true" focusable="false" />
              </div>
            </div>

            {error && (
              <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2.5">
                <AlertCircle className="h-4.5 w-4.5 text-red-500 flex-shrink-0 mt-0.5" />
                <p className="text-sm text-red-700 flex-1 leading-snug">{error}</p>
                <button onClick={() => setError(null)} className="text-red-400 hover:text-red-600 flex-shrink-0">
                  <X className="h-4 w-4" />
                </button>
              </div>
            )}

            <button
              type="submit"
              disabled={isLoading || companiesLoading || licensesLoading}
              className="w-full py-2.5 px-4 bg-gradient-to-r from-blue-600 to-blue-700 text-white font-semibold rounded-lg shadow-md hover:from-blue-700 hover:to-blue-800 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2 mt-2"
            >
              {isLoading ? (
                <>
                  <svg className="animate-spin h-4.5 w-4.5" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  Signing in...
                </>
              ) : (
                <>
                  <LogIn className="h-4 w-4" />
                  Sign In
                </>
              )}
            </button>
          </form>
        </div>
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
