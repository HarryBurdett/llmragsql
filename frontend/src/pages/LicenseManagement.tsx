import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Plus, Edit2, Trash2, Building2, Users, Server, AlertCircle, Check, X } from 'lucide-react';
import { authFetch } from '../api/client';

interface License {
  id: number;
  client_name: string;
  opera_version: 'SE' | '3';
  max_users: number;
  is_active: boolean;
  created_at: string;
  notes: string | null;
  active_sessions?: number;
}

export function LicenseManagement() {
  const queryClient = useQueryClient();
  const [showModal, setShowModal] = useState(false);
  const [editingLicense, setEditingLicense] = useState<License | null>(null);
  const [formError, setFormError] = useState<string | null>(null);

  // Form state
  const [formClientName, setFormClientName] = useState('');
  const [formOperaVersion, setFormOperaVersion] = useState<'SE' | '3'>('SE');
  const [formMaxUsers, setFormMaxUsers] = useState(5);
  const [formNotes, setFormNotes] = useState('');
  const [formIsActive, setFormIsActive] = useState(true);

  // Fetch licenses
  const { data: licensesData, isLoading, error } = useQuery({
    queryKey: ['admin-licenses'],
    queryFn: async () => {
      const response = await authFetch('http://localhost:8000/api/admin/licenses');
      if (!response.ok) throw new Error('Failed to fetch licenses');
      return response.json();
    },
  });

  // Create/Update mutation
  const saveMutation = useMutation({
    mutationFn: async (data: {
      id?: number;
      client_name: string;
      opera_version: string;
      max_users: number;
      notes: string | null;
      is_active?: boolean;
    }) => {
      const url = data.id
        ? `http://localhost:8000/api/admin/licenses/${data.id}`
        : 'http://localhost:8000/api/admin/licenses';
      const method = data.id ? 'PUT' : 'POST';

      const response = await authFetch(url, {
        method,
        body: JSON.stringify(data),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to save license');
      }
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-licenses'] });
      closeModal();
    },
    onError: (error: Error) => {
      setFormError(error.message);
    },
  });

  // Delete mutation
  const deleteMutation = useMutation({
    mutationFn: async (id: number) => {
      const response = await authFetch(`http://localhost:8000/api/admin/licenses/${id}`, {
        method: 'DELETE',
      });
      if (!response.ok) throw new Error('Failed to delete license');
      return response.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-licenses'] });
    },
  });

  const licenses: License[] = licensesData?.licenses || [];

  const openCreateModal = () => {
    setEditingLicense(null);
    setFormClientName('');
    setFormOperaVersion('SE');
    setFormMaxUsers(5);
    setFormNotes('');
    setFormIsActive(true);
    setFormError(null);
    setShowModal(true);
  };

  const openEditModal = (license: License) => {
    setEditingLicense(license);
    setFormClientName(license.client_name);
    setFormOperaVersion(license.opera_version);
    setFormMaxUsers(license.max_users);
    setFormNotes(license.notes || '');
    setFormIsActive(license.is_active);
    setFormError(null);
    setShowModal(true);
  };

  const closeModal = () => {
    setShowModal(false);
    setEditingLicense(null);
    setFormError(null);
  };

  const handleSubmit = () => {
    setFormError(null);

    if (!formClientName.trim()) {
      setFormError('Client name is required');
      return;
    }

    saveMutation.mutate({
      id: editingLicense?.id,
      client_name: formClientName.trim(),
      opera_version: formOperaVersion,
      max_users: formMaxUsers,
      notes: formNotes.trim() || null,
      is_active: editingLicense ? formIsActive : undefined,
    });
  };

  const handleDelete = (license: License) => {
    if (window.confirm(`Deactivate license for "${license.client_name}"?`)) {
      deleteMutation.mutate(license.id);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4 bg-red-50 border border-red-200 rounded-lg">
        <p className="text-red-700">Error loading licenses: {(error as Error).message}</p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">License Management</h1>
          <p className="text-sm text-gray-500 mt-1">
            Manage client licenses and Opera version assignments
          </p>
        </div>
        <button
          onClick={openCreateModal}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          <Plus className="h-4 w-4" />
          Add License
        </button>
      </div>

      {/* Licenses Table */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Client
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Opera Version
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Max Users
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Status
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Notes
              </th>
              <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {licenses.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-6 py-8 text-center text-gray-500">
                  No licenses configured. Click "Add License" to create one.
                </td>
              </tr>
            ) : (
              licenses.map((license) => (
                <tr key={license.id} className={!license.is_active ? 'bg-gray-50 opacity-60' : ''}>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center gap-2">
                      <Building2 className="h-4 w-4 text-gray-400" />
                      <span className="font-medium text-gray-900">{license.client_name}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      license.opera_version === 'SE'
                        ? 'bg-blue-100 text-blue-800'
                        : 'bg-purple-100 text-purple-800'
                    }`}>
                      <Server className="h-3 w-3" />
                      Opera {license.opera_version === 'SE' ? 'SQL SE' : '3 (FoxPro)'}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center gap-1 text-gray-600">
                      <Users className="h-4 w-4" />
                      <span>{license.max_users}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    {license.is_active ? (
                      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                        <Check className="h-3 w-3" />
                        Active
                      </span>
                    ) : (
                      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-600">
                        <X className="h-3 w-3" />
                        Inactive
                      </span>
                    )}
                  </td>
                  <td className="px-6 py-4">
                    <span className="text-sm text-gray-500 truncate max-w-xs block">
                      {license.notes || '-'}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-right">
                    <div className="flex items-center justify-end gap-2">
                      <button
                        onClick={() => openEditModal(license)}
                        className="p-1.5 text-gray-400 hover:text-blue-600 hover:bg-blue-50 rounded transition-colors"
                        title="Edit"
                      >
                        <Edit2 className="h-4 w-4" />
                      </button>
                      {license.is_active && (
                        <button
                          onClick={() => handleDelete(license)}
                          className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded transition-colors"
                          title="Deactivate"
                        >
                          <Trash2 className="h-4 w-4" />
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Create/Edit Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-gray-900 bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-full max-w-md mx-4">
            <div className="px-6 py-4 border-b border-gray-200">
              <h2 className="text-lg font-semibold text-gray-900">
                {editingLicense ? 'Edit License' : 'Add License'}
              </h2>
            </div>

            <div className="px-6 py-4 space-y-4">
              {formError && (
                <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
                  <AlertCircle className="h-5 w-5 text-red-500 flex-shrink-0 mt-0.5" />
                  <p className="text-sm text-red-700">{formError}</p>
                </div>
              )}

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Client Name <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  value={formClientName}
                  onChange={(e) => setFormClientName(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="e.g., Acme Corporation"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Opera Version <span className="text-red-500">*</span>
                </label>
                <select
                  value={formOperaVersion}
                  onChange={(e) => setFormOperaVersion(e.target.value as 'SE' | '3')}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  <option value="SE">Opera SQL SE</option>
                  <option value="3">Opera 3 (FoxPro)</option>
                </select>
                <p className="mt-1 text-xs text-gray-500">
                  Determines which companies the client can access
                </p>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Maximum Users
                </label>
                <input
                  type="number"
                  min={1}
                  max={100}
                  value={formMaxUsers}
                  onChange={(e) => setFormMaxUsers(Number(e.target.value))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
                <p className="mt-1 text-xs text-gray-500">
                  Maximum concurrent users allowed for this license
                </p>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Notes
                </label>
                <textarea
                  value={formNotes}
                  onChange={(e) => setFormNotes(e.target.value)}
                  rows={2}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="Optional notes about this license..."
                />
              </div>

              {editingLicense && (
                <div className="flex items-center gap-2">
                  <input
                    type="checkbox"
                    id="isActive"
                    checked={formIsActive}
                    onChange={(e) => setFormIsActive(e.target.checked)}
                    className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                  />
                  <label htmlFor="isActive" className="text-sm text-gray-700">
                    License is active
                  </label>
                </div>
              )}
            </div>

            <div className="px-6 py-4 border-t border-gray-200 flex justify-end gap-3">
              <button
                onClick={closeModal}
                className="px-4 py-2 text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleSubmit}
                disabled={saveMutation.isPending}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
              >
                {saveMutation.isPending ? 'Saving...' : editingLicense ? 'Save Changes' : 'Create License'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default LicenseManagement;
