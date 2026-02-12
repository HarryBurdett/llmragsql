import { createContext, useContext, useState, useEffect, type ReactNode } from 'react';
import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000/api';

// Types
export interface User {
  id: number;
  username: string;
  display_name: string;
  email: string | null;
  is_admin: boolean;
  default_company: string | null;
}

export interface License {
  id: number;
  client_name: string;
  opera_version: 'SE' | '3';
  max_users: number;
  is_active: boolean;
}

export interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  permissions: Record<string, boolean>;
  license: License | null;
  isLoading: boolean;
}

export interface AuthContextType extends AuthState {
  login: (username: string, password: string, licenseId?: number) => Promise<{ success: boolean; error?: string }>;
  logout: () => void;
  hasPermission: (module: string) => boolean;
}

// Create context
const AuthContext = createContext<AuthContextType | null>(null);

// Storage keys
const TOKEN_KEY = 'auth_token';
const USER_KEY = 'auth_user';
const PERMISSIONS_KEY = 'auth_permissions';
const LICENSE_KEY = 'auth_license';

// Provider component
export function AuthProvider({ children }: { children: ReactNode }) {
  const [authState, setAuthState] = useState<AuthState>(() => {
    // Load initial state from localStorage
    const token = localStorage.getItem(TOKEN_KEY);
    const userStr = localStorage.getItem(USER_KEY);
    const permissionsStr = localStorage.getItem(PERMISSIONS_KEY);
    const licenseStr = localStorage.getItem(LICENSE_KEY);

    let user: User | null = null;
    let permissions: Record<string, boolean> = {};
    let license: License | null = null;

    try {
      if (userStr) {
        user = JSON.parse(userStr);
      }
      if (permissionsStr) {
        permissions = JSON.parse(permissionsStr);
      }
      if (licenseStr) {
        license = JSON.parse(licenseStr);
      }
    } catch (e) {
      console.error('Error parsing auth data from localStorage:', e);
    }

    return {
      user,
      token,
      isAuthenticated: !!token && !!user,
      permissions,
      license,
      isLoading: !!token, // If we have a token, we'll validate it
    };
  });

  // Validate token on mount
  useEffect(() => {
    const validateToken = async () => {
      if (!authState.token) {
        setAuthState(prev => ({ ...prev, isLoading: false }));
        return;
      }

      try {
        const response = await axios.get(`${API_BASE_URL}/auth/me`, {
          headers: { Authorization: `Bearer ${authState.token}` },
        });

        if (response.data.success) {
          const { user, permissions } = response.data;
          // Also fetch license info for the session
          let license = authState.license;
          try {
            const licenseResponse = await axios.get(`${API_BASE_URL}/session/license`, {
              headers: { Authorization: `Bearer ${authState.token}` },
            });
            license = licenseResponse.data.license;
            if (license) {
              localStorage.setItem(LICENSE_KEY, JSON.stringify(license));
            }
          } catch {
            // License fetch failed, use stored value
          }

          setAuthState({
            user,
            token: authState.token,
            isAuthenticated: true,
            permissions,
            license,
            isLoading: false,
          });
          // Update localStorage
          localStorage.setItem(USER_KEY, JSON.stringify(user));
          localStorage.setItem(PERMISSIONS_KEY, JSON.stringify(permissions));
        } else {
          // Token invalid, clear state
          clearAuthState();
        }
      } catch (error) {
        // Token invalid or expired
        clearAuthState();
      }
    };

    validateToken();
  }, []); // Only run on mount

  const clearAuthState = () => {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
    localStorage.removeItem(PERMISSIONS_KEY);
    localStorage.removeItem(LICENSE_KEY);
    setAuthState({
      user: null,
      token: null,
      isAuthenticated: false,
      permissions: {},
      license: null,
      isLoading: false,
    });
  };

  const login = async (username: string, password: string, licenseId?: number): Promise<{ success: boolean; error?: string }> => {
    try {
      const response = await axios.post(`${API_BASE_URL}/auth/login`, {
        username,
        password,
        license_id: licenseId,
      });

      if (response.data.success) {
        const { token, user, permissions, license } = response.data;

        // Save to localStorage
        localStorage.setItem(TOKEN_KEY, token);
        localStorage.setItem(USER_KEY, JSON.stringify(user));
        localStorage.setItem(PERMISSIONS_KEY, JSON.stringify(permissions));
        if (license) {
          localStorage.setItem(LICENSE_KEY, JSON.stringify(license));
        } else {
          localStorage.removeItem(LICENSE_KEY);
        }

        // Update state
        setAuthState({
          user,
          token,
          isAuthenticated: true,
          permissions,
          license: license || null,
          isLoading: false,
        });

        return { success: true };
      } else {
        return { success: false, error: response.data.error || 'Login failed' };
      }
    } catch (error: unknown) {
      const err = error as { response?: { data?: { error?: string; detail?: string } } };
      const errorMessage = err.response?.data?.error || err.response?.data?.detail || 'Login failed';
      return { success: false, error: errorMessage };
    }
  };

  const logout = async () => {
    // Call logout API (best effort)
    try {
      if (authState.token) {
        await axios.post(
          `${API_BASE_URL}/auth/logout`,
          {},
          { headers: { Authorization: `Bearer ${authState.token}` } }
        );
      }
    } catch (e) {
      // Ignore errors
    }

    // Clear state
    clearAuthState();
  };

  const hasPermission = (module: string): boolean => {
    // Admins have all permissions
    if (authState.user?.is_admin) {
      return true;
    }
    return authState.permissions[module] === true;
  };

  return (
    <AuthContext.Provider
      value={{
        ...authState,
        login,
        logout,
        hasPermission,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

// Hook to use auth context
export function useAuth(): AuthContextType {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}

export default AuthContext;
