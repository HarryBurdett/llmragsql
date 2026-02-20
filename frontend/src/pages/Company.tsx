import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Building2, Check, RefreshCw, Search, Download, FolderInput, Database, CheckSquare, Square } from 'lucide-react';
import apiClient, { authFetch, ScannedDatabase } from '../api/client';
import { useAuth } from '../context/AuthContext';
import { PageHeader, Card, Alert, LoadingState, EmptyState, StatusBadge } from '../components/ui';

export function Company() {
  const { user } = useAuth();
  const queryClient = useQueryClient();
  const [discoverMessage, setDiscoverMessage] = useState<string | null>(null);
  const [discoverError, setDiscoverError] = useState<string | null>(null);
  const [switchMessage, setSwitchMessage] = useState<string | null>(null);

  // Import learned data state
  const [importSourcePath, setImportSourcePath] = useState<string>(
    () => localStorage.getItem('importLearnedData_sourcePath') || ''
  );
  const [scannedDatabases, setScannedDatabases] = useState<ScannedDatabase[]>([]);
  const [selectedDatabases, setSelectedDatabases] = useState<Set<string>>(new Set());
  const [scanSourceLocation, setScanSourceLocation] = useState<string | null>(null);
  const [importMessage, setImportMessage] = useState<string | null>(null);
  const [importError, setImportError] = useState<string | null>(null);

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

  const scanMutation = useMutation({
    mutationFn: async () => {
      const companyId = currentCompany?.id;
      if (!companyId) throw new Error('No active company');
      const response = await apiClient.scanLearnedData(importSourcePath, companyId);
      return response.data;
    },
    onSuccess: (data) => {
      setScannedDatabases(data.databases);
      setScanSourceLocation(data.source_location);
      setImportError(null);
      // Pre-select defaults
      const defaults = new Set<string>();
      data.databases.forEach(db => {
        if (db.default_selected) defaults.add(db.name);
      });
      setSelectedDatabases(defaults);
      // Persist source path
      localStorage.setItem('importLearnedData_sourcePath', importSourcePath);
    },
    onError: (error: Error) => {
      setScannedDatabases([]);
      setScanSourceLocation(null);
      setImportError(error.message);
    }
  });

  const importMutation = useMutation({
    mutationFn: async () => {
      const companyId = currentCompany?.id;
      if (!companyId) throw new Error('No active company');
      const response = await apiClient.importLearnedData(
        importSourcePath,
        companyId,
        Array.from(selectedDatabases)
      );
      return response.data;
    },
    onSuccess: (data) => {
      const count = data.imported.length;
      const records = data.imported.map(d => `${d.name} (${d.record_count ?? 0} records)`).join(', ');
      setImportMessage(`Imported ${count} database${count !== 1 ? 's' : ''}: ${records}`);
      setImportError(null);
      setScannedDatabases([]);
      if (data.errors.length > 0) {
        setImportError(data.errors.join('; '));
      }
      setTimeout(() => setImportMessage(null), 15000);
    },
    onError: (error: Error) => {
      setImportError(error.message);
    }
  });

  const companies = companiesData?.companies || [];
  const currentCompany = companiesData?.current_company;

  const handleSwitchCompany = (companyId: string) => {
    if (companyId !== currentCompany?.id) {
      switchMutation.mutate(companyId);
    }
  };

  const toggleDatabase = (dbName: string) => {
    setSelectedDatabases(prev => {
      const next = new Set(prev);
      if (next.has(dbName)) {
        next.delete(dbName);
      } else {
        next.add(dbName);
      }
      return next;
    });
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

      {/* Import Learned Data - admin only */}
      {user?.is_admin && currentCompany && (
        <Card>
          <div className="flex items-center gap-2 mb-2">
            <FolderInput className="h-5 w-5 text-gray-600" />
            <h3 className="text-base font-semibold text-gray-900">Import Learned Data</h3>
          </div>
          <p className="text-sm text-gray-500 mb-4">
            Copy learned bank patterns and aliases from another SQL RAG installation into <strong>{currentCompany.name}</strong>.
            Works with both Opera SQL SE and Opera 3 installations.
          </p>

          {importMessage && (
            <Alert variant="success" onDismiss={() => setImportMessage(null)}>
              {importMessage}
            </Alert>
          )}

          {importError && (
            <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
              <span className="text-red-500 mt-0.5">!</span>
              <div className="flex-1">
                <p className="text-sm text-red-800">{importError}</p>
              </div>
              <button onClick={() => setImportError(null)} className="text-red-400 hover:text-red-600 text-lg leading-none">&times;</button>
            </div>
          )}

          {/* Source path input */}
          <div className="flex gap-2 mb-4">
            <input
              type="text"
              value={importSourcePath}
              onChange={e => setImportSourcePath(e.target.value)}
              placeholder="Path to source SQL RAG installation (e.g., /path/to/llmragsql)"
              className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            />
            <button
              onClick={() => scanMutation.mutate()}
              disabled={scanMutation.isPending || !importSourcePath.trim()}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 whitespace-nowrap"
            >
              {scanMutation.isPending ? (
                <RefreshCw className="h-4 w-4 animate-spin" />
              ) : (
                <Search className="h-4 w-4" />
              )}
              Scan
            </button>
          </div>

          {/* Scan results */}
          {scannedDatabases.length > 0 && (
            <div className="space-y-3">
              {scanSourceLocation && (
                <p className="text-xs text-gray-500">
                  Source: {scanSourceLocation}
                </p>
              )}

              <div className="border border-gray-200 rounded-lg overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 border-b border-gray-200">
                    <tr>
                      <th className="px-4 py-2 text-left w-8"></th>
                      <th className="px-4 py-2 text-left">Database</th>
                      <th className="px-4 py-2 text-left">Description</th>
                      <th className="px-4 py-2 text-right">Records</th>
                      <th className="px-4 py-2 text-right">Size</th>
                    </tr>
                  </thead>
                  <tbody>
                    {scannedDatabases.map((db) => (
                      <tr
                        key={db.name}
                        onClick={() => toggleDatabase(db.name)}
                        className="border-b border-gray-100 hover:bg-blue-50/30 cursor-pointer"
                      >
                        <td className="px-4 py-2">
                          {selectedDatabases.has(db.name) ? (
                            <CheckSquare className="h-4 w-4 text-blue-600" />
                          ) : (
                            <Square className="h-4 w-4 text-gray-400" />
                          )}
                        </td>
                        <td className="px-4 py-2 font-medium flex items-center gap-2">
                          <Database className="h-3.5 w-3.5 text-gray-400" />
                          {db.name}
                        </td>
                        <td className="px-4 py-2 text-gray-600">{db.description}</td>
                        <td className="px-4 py-2 text-right font-mono text-gray-700">
                          {db.record_count !== null ? db.record_count.toLocaleString() : '-'}
                        </td>
                        <td className="px-4 py-2 text-right text-gray-500">{db.size_display}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              <div className="flex items-center justify-between">
                <p className="text-xs text-gray-500">
                  {selectedDatabases.size} of {scannedDatabases.length} selected. Existing databases will be backed up before overwriting.
                </p>
                <button
                  onClick={() => importMutation.mutate()}
                  disabled={importMutation.isPending || selectedDatabases.size === 0}
                  className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50"
                >
                  {importMutation.isPending ? (
                    <RefreshCw className="h-4 w-4 animate-spin" />
                  ) : (
                    <Download className="h-4 w-4" />
                  )}
                  Import Selected
                </button>
              </div>
            </div>
          )}
        </Card>
      )}
    </div>
  );
}
