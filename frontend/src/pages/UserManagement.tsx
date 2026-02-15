import { useState, useEffect } from 'react';
import { Users, Plus, Edit, Trash2, Shield, X, Check, Eye, RefreshCw, Download, Building2 } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000/api';

interface User {
  id: number;
  username: string;
  display_name: string;
  email: string | null;
  is_admin: boolean;
  is_active: boolean;
  permissions: Record<string, boolean>;
  created_at: string | null;
  last_login: string | null;
  created_by: string | null;
  default_company: string | null;
  company_access: string[];  // List of company IDs user can access (empty = all)
}

interface Company {
  id: string;
  name: string;
  description: string;
}

interface Module {
  id: string;
  name: string;
  description: string;
}

const MODULES: Module[] = [
  { id: 'cashbook', name: 'Cashbook', description: 'Bank Reconciliation, GoCardless Import' },
  { id: 'payroll', name: 'Payroll', description: 'Pension Export, Parameters' },
  { id: 'ap_automation', name: 'AP Automation', description: 'Supplier Statement Automation' },
  { id: 'utilities', name: 'Utilities', description: 'Balance Check, User Activity' },
  { id: 'development', name: 'Development', description: 'Opera SE, Archive' },
  { id: 'administration', name: 'Administration', description: 'Company, Projects, Lock Monitor, Settings' },
];

export function UserManagement() {
  const { token, user: currentUser } = useAuth();
  const [users, setUsers] = useState<User[]>([]);
  const [companies, setCompanies] = useState<Company[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [editingUser, setEditingUser] = useState<User | null>(null);

  // Form state
  const [formUsername, setFormUsername] = useState('');
  const [formPassword, setFormPassword] = useState('');
  const [formDisplayName, setFormDisplayName] = useState('');
  const [formEmail, setFormEmail] = useState('');
  const [formIsAdmin, setFormIsAdmin] = useState(false);
  const [formPermissions, setFormPermissions] = useState<Record<string, boolean>>({});
  const [formDefaultCompany, setFormDefaultCompany] = useState('');
  const [formError, setFormError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const [revealedPassword, setRevealedPassword] = useState<string | null>(null);
  const [isSyncing, setIsSyncing] = useState(false);
  const [syncMessage, setSyncMessage] = useState<string | null>(null);

  // Company access modal state
  const [isCompanyModalOpen, setIsCompanyModalOpen] = useState(false);
  const [companyModalUser, setCompanyModalUser] = useState<User | null>(null);
  const [selectedCompanyAccess, setSelectedCompanyAccess] = useState<string[]>([]);
  const [isSavingCompanies, setIsSavingCompanies] = useState(false);

  // Axios instance with auth header
  const api = axios.create({
    baseURL: API_BASE_URL,
    headers: { Authorization: `Bearer ${token}` },
  });

  // Fetch users
  const fetchUsers = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await api.get('/admin/users');
      if (response.data.success) {
        setUsers(response.data.users);
      } else {
        setError('Failed to load users');
      }
    } catch (err: unknown) {
      const error = err as { response?: { data?: { detail?: string } } };
      setError(error.response?.data?.detail || 'Failed to load users');
    } finally {
      setIsLoading(false);
    }
  };

  // Fetch companies for the dropdown (use unfiltered list for admin)
  const fetchCompanies = async () => {
    try {
      // Use /companies/list for admin - this is the unfiltered list
      const response = await api.get('/companies/list');
      if (response.data.companies) {
        setCompanies(response.data.companies);
      }
    } catch (err) {
      console.error('Failed to load companies:', err);
    }
  };

  useEffect(() => {
    fetchUsers();
    fetchCompanies();
  }, []);

  // Sync users from Opera
  const syncFromOpera = async () => {
    setIsSyncing(true);
    setSyncMessage(null);
    setError(null);
    try {
      const response = await api.post('/admin/users/sync-from-opera');
      if (response.data.success) {
        const created = response.data.created?.length || 0;
        const updated = response.data.updated?.length || 0;
        const errors = response.data.errors?.length || 0;

        let message = `Synced from Opera: ${created} new users created`;
        if (updated > 0) {
          message += `, ${updated} users updated`;
        }
        if (errors > 0) {
          message += ` (${errors} errors)`;
        }
        message += '. Permissions mapped from Opera NavGroups.';

        setSyncMessage(message);
        if (created > 0 || updated > 0) {
          fetchUsers(); // Refresh user list
        }
      } else {
        setError(response.data.error || 'Failed to sync users');
      }
    } catch (err: unknown) {
      const error = err as { response?: { data?: { detail?: string; error?: string } } };
      setError(error.response?.data?.detail || error.response?.data?.error || 'Failed to sync users from Opera');
    } finally {
      setIsSyncing(false);
    }
  };

  // Reset form
  const resetForm = () => {
    setFormUsername('');
    setFormPassword('');
    setFormDisplayName('');
    setFormEmail('');
    setFormIsAdmin(false);
    setFormPermissions({});
    setFormDefaultCompany('');
    setFormError(null);
    setEditingUser(null);
    setShowPassword(false);
    setRevealedPassword(null);
  };

  // Open modal for new user
  const openNewUserModal = () => {
    resetForm();
    setIsModalOpen(true);
  };

  // Open modal for editing user
  const openEditUserModal = (user: User) => {
    setEditingUser(user);
    setFormUsername(user.username);
    setFormPassword(''); // Don't pre-fill password
    setFormDisplayName(user.display_name);
    setFormEmail(user.email || '');
    setFormIsAdmin(user.is_admin);
    setFormPermissions({ ...user.permissions });
    setFormDefaultCompany(user.default_company || '');
    setFormError(null);
    setIsModalOpen(true);
  };

  // Close modal
  const closeModal = () => {
    setIsModalOpen(false);
    resetForm();
    // Blur any focused element to ensure clean state for dropdowns
    if (document.activeElement instanceof HTMLElement) {
      document.activeElement.blur();
    }
  };

  // Handle Escape key to close modal - use capture phase to catch before other handlers
  useEffect(() => {
    if (!isModalOpen) return;

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();
        closeModal();
      }
    };

    // Use capture phase to ensure this fires first
    document.addEventListener('keydown', handleEscape, true);
    return () => document.removeEventListener('keydown', handleEscape, true);
  }, [isModalOpen]);

  // Toggle permission
  const togglePermission = (moduleId: string) => {
    setFormPermissions((prev) => ({
      ...prev,
      [moduleId]: !prev[moduleId],
    }));
  };

  // Handle admin toggle - if admin, grant all permissions
  const handleAdminToggle = () => {
    const newIsAdmin = !formIsAdmin;
    setFormIsAdmin(newIsAdmin);
    if (newIsAdmin) {
      // Grant all permissions
      const allPermissions: Record<string, boolean> = {};
      MODULES.forEach((m) => {
        allPermissions[m.id] = true;
      });
      setFormPermissions(allPermissions);
    }
  };

  // Save user (create or update)
  const handleSave = async () => {
    setFormError(null);

    // Validation
    if (!formUsername.trim()) {
      setFormError('Username is required');
      return;
    }
    if (!editingUser && !formPassword) {
      setFormError('Password is required for new users');
      return;
    }
    // Note: default_company is synced from Opera, not editable here

    setIsSaving(true);

    try {
      if (editingUser) {
        // Update existing user
        // Note: default_company is synced from Opera, not editable here
        const updateData: Record<string, unknown> = {
          username: formUsername,
          display_name: formDisplayName || null,
          email: formEmail || null,
          is_admin: formIsAdmin,
          permissions: formPermissions,
        };
        if (formPassword) {
          updateData.password = formPassword;
        }

        const response = await api.put(`/admin/users/${editingUser.id}`, updateData);
        if (response.data.success) {
          closeModal();
          fetchUsers();
        } else {
          setFormError('Failed to update user');
        }
      } else {
        // Create new user
        // Note: default_company is synced from Opera, not editable here
        const response = await api.post('/admin/users', {
          username: formUsername,
          password: formPassword,
          display_name: formDisplayName || null,
          email: formEmail || null,
          is_admin: formIsAdmin,
          permissions: formPermissions,
        });
        if (response.data.success) {
          closeModal();
          fetchUsers();
        } else {
          setFormError('Failed to create user');
        }
      }
    } catch (err: unknown) {
      const error = err as { response?: { data?: { detail?: string } } };
      setFormError(error.response?.data?.detail || 'Failed to save user');
    } finally {
      setIsSaving(false);
    }
  };

  // Delete (deactivate) user
  const handleDelete = async (userId: number) => {
    if (!confirm('Are you sure you want to deactivate this user?')) {
      return;
    }

    try {
      const response = await api.delete(`/admin/users/${userId}`);
      if (response.data.success) {
        fetchUsers();
      }
    } catch (err: unknown) {
      const error = err as { response?: { data?: { detail?: string } } };
      alert(error.response?.data?.detail || 'Failed to deactivate user');
    }
  };

  // Format date
  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return '-';
    try {
      return new Date(dateStr).toLocaleDateString('en-GB', {
        day: '2-digit',
        month: 'short',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      });
    } catch {
      return dateStr;
    }
  };

  // Reveal user password (admin only) - for use in edit form
  const handleRevealPassword = async (userId: number) => {
    try {
      const response = await api.get(`/admin/users/${userId}/password`);
      if (response.data.success) {
        setRevealedPassword(response.data.password);
        setShowPassword(true);
      }
    } catch (err: unknown) {
      const error = err as { response?: { data?: { detail?: string } } };
      setFormError(error.response?.data?.detail || 'Could not retrieve password');
    }
  };

  // Open company access modal
  const openCompanyModal = (user: User) => {
    setCompanyModalUser(user);
    setSelectedCompanyAccess([...user.company_access]);
    setIsCompanyModalOpen(true);
  };

  // Close company access modal
  const closeCompanyModal = () => {
    setIsCompanyModalOpen(false);
    setCompanyModalUser(null);
    setSelectedCompanyAccess([]);
  };

  // Toggle company access
  const toggleCompanyAccess = (companyId: string) => {
    setSelectedCompanyAccess((prev) =>
      prev.includes(companyId)
        ? prev.filter((id) => id !== companyId)
        : [...prev, companyId]
    );
  };

  // Save company access
  const handleSaveCompanyAccess = async () => {
    if (!companyModalUser) return;

    setIsSavingCompanies(true);
    try {
      const response = await api.put(`/admin/users/${companyModalUser.id}/companies`, {
        companies: selectedCompanyAccess,
      });
      if (response.data.success) {
        closeCompanyModal();
        fetchUsers(); // Refresh user list
      }
    } catch (err: unknown) {
      const error = err as { response?: { data?: { detail?: string } } };
      alert(error.response?.data?.detail || 'Failed to save company access');
    } finally {
      setIsSavingCompanies(false);
    }
  };

  // Handle Escape key to close company modal
  useEffect(() => {
    if (!isCompanyModalOpen) return;

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        e.stopPropagation();
        closeCompanyModal();
      }
    };

    document.addEventListener('keydown', handleEscape, true);
    return () => document.removeEventListener('keydown', handleEscape, true);
  }, [isCompanyModalOpen]);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-blue-100 rounded-lg">
            <Users className="h-6 w-6 text-blue-600" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900">User Management</h1>
            <p className="text-sm text-gray-500">Manage users and their permissions</p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={syncFromOpera}
            disabled={isSyncing}
            className="flex items-center gap-2 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors disabled:opacity-50"
            title="Import users and permissions from Opera SE. Module permissions are mapped from Opera NavGroups."
          >
            {isSyncing ? (
              <RefreshCw className="h-4 w-4 animate-spin" />
            ) : (
              <Download className="h-4 w-4" />
            )}
            Sync from Opera
          </button>
          <button
            onClick={openNewUserModal}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            <Plus className="h-4 w-4" />
            Add User
          </button>
        </div>
      </div>

      {/* Sync success message */}
      {syncMessage && (
        <div className="p-4 bg-green-50 border border-green-200 rounded-lg text-green-700 flex items-center gap-2">
          <Check className="h-4 w-4" />
          {syncMessage}
          <button onClick={() => setSyncMessage(null)} className="ml-auto text-green-500 hover:text-green-700">
            <X className="h-4 w-4" />
          </button>
        </div>
      )}

      {/* Error message */}
      {error && (
        <div className="p-4 bg-red-50 border border-red-200 rounded-lg text-red-700">
          {error}
        </div>
      )}

      {/* Users table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        {isLoading ? (
          <div className="p-8 text-center text-gray-500">Loading users...</div>
        ) : users.length === 0 ? (
          <div className="p-8 text-center text-gray-500">No users found</div>
        ) : (
          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  User
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Permissions
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Last Login
                </th>
                <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {users.map((user) => (
                <tr key={user.id} className={!user.is_active ? 'bg-gray-50 opacity-60' : ''}>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      <div className="flex-shrink-0 h-10 w-10 rounded-full bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center text-white font-semibold">
                        {user.display_name.charAt(0).toUpperCase()}
                      </div>
                      <div className="ml-4">
                        <div className="text-sm font-medium text-gray-900">
                          {user.display_name}
                          {user.is_admin && (
                            <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-800">
                              <Shield className="h-3 w-3 mr-1" />
                              Admin
                            </span>
                          )}
                        </div>
                        <div className="text-sm text-gray-500">@{user.username}</div>
                        {user.default_company && (
                          <div className="text-xs text-blue-600">
                            Default: {companies.find(c => c.id === user.default_company)?.name || user.default_company}
                          </div>
                        )}
                        {user.company_access && user.company_access.length > 0 && !user.is_admin && (
                          <div className="text-xs text-green-600 flex items-center gap-1">
                            <Building2 className="h-3 w-3" />
                            {user.company_access.length} {user.company_access.length === 1 ? 'company' : 'companies'}
                          </div>
                        )}
                      </div>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span
                      className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                        user.is_active
                          ? 'bg-green-100 text-green-800'
                          : 'bg-red-100 text-red-800'
                      }`}
                    >
                      {user.is_active ? 'Active' : 'Inactive'}
                    </span>
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex flex-wrap gap-1">
                      {user.is_admin ? (
                        <span className="text-xs text-purple-600">All modules</span>
                      ) : (
                        MODULES.filter((m) => user.permissions[m.id]).map((m) => (
                          <span
                            key={m.id}
                            className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-gray-100 text-gray-700"
                          >
                            {m.name}
                          </span>
                        ))
                      )}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {formatDate(user.last_login)}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                    <button
                      onClick={() => openCompanyModal(user)}
                      className="text-green-600 hover:text-green-900 mr-3"
                      title="Manage company access"
                    >
                      <Building2 className="h-4 w-4" />
                    </button>
                    <button
                      onClick={() => openEditUserModal(user)}
                      className="text-blue-600 hover:text-blue-900 mr-3"
                      title="Edit user"
                    >
                      <Edit className="h-4 w-4" />
                    </button>
                    {user.id !== currentUser?.id && (
                      <button
                        onClick={() => handleDelete(user.id)}
                        className="text-red-600 hover:text-red-900"
                        title="Deactivate user"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Modal */}
      {isModalOpen && (
        <div
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50"
          onClick={(e) => {
            // Close modal when clicking backdrop (not the modal content)
            if (e.target === e.currentTarget) {
              closeModal();
            }
          }}
        >
          <div className="bg-white rounded-xl shadow-xl w-full max-w-lg max-h-[90vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
            {/* Modal header */}
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900">
                {editingUser ? 'Edit User' : 'Add New User'}
              </h3>
              <button
                onClick={closeModal}
                className="text-gray-400 hover:text-gray-600 transition-colors"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            {/* Modal body */}
            <div className="px-6 py-4 space-y-4">
              {/* Username */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Username <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  value={formUsername}
                  onChange={(e) => setFormUsername(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  placeholder="Enter username"
                />
              </div>

              {/* Password */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Password {!editingUser && <span className="text-red-500">*</span>}
                </label>
                <div className="flex gap-2">
                  <input
                    type={showPassword ? 'text' : 'password'}
                    value={formPassword}
                    onChange={(e) => {
                      setFormPassword(e.target.value);
                      setRevealedPassword(null); // Clear revealed when typing
                    }}
                    className="flex-1 px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder={editingUser ? 'Leave blank to keep current' : 'Enter password'}
                  />
                  {editingUser && (
                    <button
                      type="button"
                      onClick={() => {
                        if (revealedPassword) {
                          // Toggle visibility
                          setShowPassword(!showPassword);
                        } else {
                          // Fetch and reveal
                          handleRevealPassword(editingUser.id);
                        }
                      }}
                      className="px-3 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors text-gray-600"
                      title={showPassword ? 'Hide password' : 'Reveal current password'}
                    >
                      <Eye className={`h-4 w-4 ${showPassword ? 'text-blue-600' : ''}`} />
                    </button>
                  )}
                </div>
                {revealedPassword && showPassword && (
                  <p className="text-xs text-blue-600 mt-1">
                    Current password: <span className="font-mono font-medium">{revealedPassword}</span>
                  </p>
                )}
                {editingUser && !revealedPassword && (
                  <p className="text-xs text-gray-500 mt-1">
                    Leave blank to keep current password, or click the eye to reveal it
                  </p>
                )}
              </div>

              {/* Display Name */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Display Name
                </label>
                <input
                  type="text"
                  value={formDisplayName}
                  onChange={(e) => setFormDisplayName(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  placeholder="Enter display name"
                />
              </div>

              {/* Email */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Email</label>
                <input
                  type="email"
                  value={formEmail}
                  onChange={(e) => setFormEmail(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  placeholder="Enter email address"
                />
              </div>

              {/* Is Admin */}
              <div className="flex items-center">
                <input
                  type="checkbox"
                  id="isAdmin"
                  checked={formIsAdmin}
                  onChange={handleAdminToggle}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                />
                <label htmlFor="isAdmin" className="ml-2 block text-sm text-gray-900">
                  Administrator <span className="text-gray-500">(full access to all modules)</span>
                </label>
              </div>

              {/* Default Company - Read-only (synced from Opera) */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Default Company
                </label>
                <div className="w-full px-3 py-2 border border-gray-200 rounded-lg bg-gray-50 text-gray-700">
                  {formDefaultCompany
                    ? companies.find(c => c.id === formDefaultCompany)?.name || formDefaultCompany
                    : <span className="text-gray-400 italic">Not set in Opera</span>
                  }
                </div>
                <p className="text-xs text-gray-500 mt-1">
                  Synced from Opera - edit in Opera to change
                </p>
              </div>

              {/* Module Permissions */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Module Permissions
                </label>
                <p className="text-xs text-gray-500 mb-2">
                  When syncing from Opera, permissions are mapped from Opera NavGroups (e.g., NavGroupPayrollManagement → Payroll).
                  Users without access to a module in Opera won't see that data in SQL RAG.
                </p>
                <div className="space-y-2 border border-gray-200 rounded-lg p-3 bg-gray-50">
                  {MODULES.map((module) => (
                    <label
                      key={module.id}
                      className="flex items-start gap-3 p-2 rounded hover:bg-white transition-colors cursor-pointer"
                    >
                      <input
                        type="checkbox"
                        checked={formIsAdmin || formPermissions[module.id] === true}
                        onChange={() => togglePermission(module.id)}
                        disabled={formIsAdmin}
                        className="h-4 w-4 mt-0.5 text-blue-600 focus:ring-blue-500 border-gray-300 rounded disabled:opacity-50"
                      />
                      <div>
                        <div className="text-sm font-medium text-gray-900">{module.name}</div>
                        <div className="text-xs text-gray-500">{module.description}</div>
                      </div>
                    </label>
                  ))}
                </div>
              </div>

              {/* Form error */}
              {formError && (
                <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
                  {formError}
                </div>
              )}
            </div>

            {/* Modal footer */}
            <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50">
              <button
                onClick={closeModal}
                className="px-4 py-2 text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleSave}
                disabled={isSaving}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
              >
                {isSaving ? (
                  <>
                    <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24">
                      <circle
                        className="opacity-25"
                        cx="12"
                        cy="12"
                        r="10"
                        stroke="currentColor"
                        strokeWidth="4"
                        fill="none"
                      />
                      <path
                        className="opacity-75"
                        fill="currentColor"
                        d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                      />
                    </svg>
                    Saving...
                  </>
                ) : (
                  <>
                    <Check className="h-4 w-4" />
                    {editingUser ? 'Update User' : 'Create User'}
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Company Access Modal - View Only (synced from Opera) */}
      {isCompanyModalOpen && companyModalUser && (
        <div
          className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50"
          onClick={(e) => {
            if (e.target === e.currentTarget) {
              closeCompanyModal();
            }
          }}
        >
          <div className="bg-white rounded-xl shadow-xl w-full max-w-md max-h-[90vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
            {/* Modal header */}
            <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-green-100 rounded-lg">
                  <Building2 className="h-5 w-5 text-green-600" />
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-gray-900">Company Access</h3>
                  <p className="text-sm text-gray-500">{companyModalUser.display_name}</p>
                </div>
              </div>
              <button
                onClick={closeCompanyModal}
                className="text-gray-400 hover:text-gray-600 transition-colors"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            {/* Modal body */}
            <div className="px-6 py-4 space-y-4">
              <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
                <p className="text-sm text-blue-700">
                  Company access is synced from Opera. To change access, update the user's permissions in Opera.
                </p>
              </div>

              {companyModalUser.is_admin && (
                <div className="p-3 bg-purple-50 border border-purple-200 rounded-lg">
                  <p className="text-sm text-purple-700 flex items-center gap-2">
                    <Shield className="h-4 w-4" />
                    Admins have access to all companies.
                  </p>
                </div>
              )}

              <div className="space-y-2 border border-gray-200 rounded-lg p-3 bg-gray-50">
                {companies.length === 0 ? (
                  <p className="text-sm text-gray-500 text-center py-4">No companies available</p>
                ) : companyModalUser.company_access.length === 0 ? (
                  <p className="text-sm text-gray-600 text-center py-4">
                    User has access to <strong>all companies</strong>
                  </p>
                ) : (
                  companies
                    .filter((company) => companyModalUser.company_access.includes(company.id))
                    .map((company) => (
                      <div
                        key={company.id}
                        className="flex items-start gap-3 p-3 rounded-lg bg-green-50 border border-green-200"
                      >
                        <Check className="h-4 w-4 mt-0.5 text-green-600" />
                        <div className="flex-1">
                          <div className="text-sm font-medium text-gray-900">{company.name}</div>
                          {company.description && (
                            <div className="text-xs text-gray-500">{company.description}</div>
                          )}
                        </div>
                        {company.id === companyModalUser.default_company && (
                          <span className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded">Default</span>
                        )}
                      </div>
                    ))
                )}
              </div>

              {companyModalUser.company_access.length > 0 && (
                <div className="text-sm text-gray-600">
                  Access to: <strong>{companyModalUser.company_access.length}</strong> of {companies.length} companies
                </div>
              )}
            </div>

            {/* Modal footer */}
            <div className="flex items-center justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50">
              <button
                onClick={closeCompanyModal}
                className="px-4 py-2 text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
