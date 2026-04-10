import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Package, ArrowRight, CheckCircle, Copy, ChevronDown, ChevronRight,
  FileText, Database, Lock, Settings, Code2, RefreshCw, Download
} from 'lucide-react';
import { authFetch } from '../api/client';
import { PageHeader, Card, Alert } from '../components/ui';

interface ModuleSummary {
  module_id: string;
  name: string;
  description: string;
  dependencies: string[];
  settings_count: number;
  data_stores: string[];
  endpoints: string[];
  pages: { path: string; label: string }[];
  locking_rules: Record<string, string>;
  file_counts: Record<string, number>;
}

export default function MigrationAssistant() {
  const [expandedModule, setExpandedModule] = useState<string | null>(null);
  const [copiedCode, setCopiedCode] = useState<string | null>(null);
  const [exportData, setExportData] = useState<any>(null);
  const [exporting, setExporting] = useState<string | null>(null);

  const { data: modulesData } = useQuery({
    queryKey: ['migration-modules'],
    queryFn: async () => { const r = await authFetch('/api/migration/modules'); return r.json(); },
  });

  const modules: ModuleSummary[] = modulesData?.modules || [];

  const loadModuleDetail = async (moduleId: string) => {
    if (expandedModule === moduleId) {
      setExpandedModule(null);
      setExportData(null);
      return;
    }
    setExpandedModule(moduleId);
    setExporting(moduleId);
    try {
      const r = await authFetch(`/api/migration/export/${moduleId}`);
      const data = await r.json();
      if (data.success) setExportData(data);
    } finally {
      setExporting(null);
    }
  };

  const copyToClipboard = (text: string, label: string) => {
    navigator.clipboard.writeText(text);
    setCopiedCode(label);
    setTimeout(() => setCopiedCode(null), 2000);
  };

  return (
    <div className="space-y-6">
      <PageHeader title="Migration Assistant" subtitle="Package modules for deployment to another platform" icon={Package} />

      {/* Summary */}
      <Card>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold text-gray-900">{modules.length} Modules Available</h2>
        </div>
        <p className="text-sm text-gray-500">
          Each module is self-contained with its own settings, data, locking rules, and integration code.
          Click a module to see everything needed to migrate it to another platform.
        </p>
      </Card>

      {/* Module list */}
      <div className="space-y-3">
        {modules.map(mod => {
          const isExpanded = expandedModule === mod.module_id;
          const totalFiles = Object.values(mod.file_counts).reduce((a, b) => a + b, 0);

          return (
            <Card key={mod.module_id} className={isExpanded ? 'ring-2 ring-blue-200' : ''}>
              {/* Header */}
              <div
                className="flex items-center justify-between cursor-pointer"
                onClick={() => loadModuleDetail(mod.module_id)}
              >
                <div className="flex items-center gap-3">
                  {isExpanded ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}
                  <div>
                    <div className="font-semibold text-gray-900">{mod.name}</div>
                    <div className="text-xs text-gray-500">{mod.description?.slice(0, 80)}</div>
                  </div>
                </div>
                <div className="flex items-center gap-4 text-xs text-gray-500">
                  <span>{totalFiles} files</span>
                  <span>{mod.endpoints.length} endpoints</span>
                  <span>{mod.pages.length} pages</span>
                  <span>{mod.settings_count} settings</span>
                </div>
              </div>

              {/* Expanded detail */}
              {isExpanded && exporting === mod.module_id && (
                <div className="mt-4 flex items-center justify-center py-8">
                  <RefreshCw className="w-5 h-5 text-blue-500 animate-spin" />
                </div>
              )}

              {isExpanded && exportData && !exporting && (
                <div className="mt-4 pt-4 border-t border-gray-100 space-y-5">

                  {/* Migration Steps */}
                  <div>
                    <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2 mb-2">
                      <ArrowRight className="w-4 h-4" /> Migration Steps
                    </h3>
                    <div className="space-y-1">
                      {exportData.migration_steps?.map((step: string, i: number) => (
                        <div key={i} className="flex items-start gap-2 text-sm">
                          <span className="text-blue-600 font-mono text-xs mt-0.5 w-5 flex-shrink-0">{i + 1}.</span>
                          <span className="text-gray-700">{step.replace(/^\d+\.\s*/, '')}</span>
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* Files */}
                  <div>
                    <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2 mb-2">
                      <FileText className="w-4 h-4" /> Files to Copy
                    </h3>
                    {Object.entries(exportData.files || {}).map(([category, files]: [string, any]) => (
                      files.length > 0 && (
                        <div key={category} className="mb-2">
                          <div className="text-xs font-medium text-gray-500 uppercase mb-1">{category.replace('_', ' ')} ({files.length})</div>
                          <div className="bg-gray-50 rounded p-2 text-xs font-mono space-y-0.5 max-h-32 overflow-y-auto">
                            {files.map((f: any) => (
                              <div key={f.path} className="flex justify-between">
                                <span className="text-gray-700">{f.path}</span>
                                <span className="text-gray-400">{f.size_formatted}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )
                    ))}
                  </div>

                  {/* Integration Code */}
                  <div>
                    <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2 mb-2">
                      <Code2 className="w-4 h-4" /> Integration Code
                    </h3>

                    {/* Router registration */}
                    <div className="mb-3">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs font-medium text-gray-500">Add to main.py (Python)</span>
                        <button onClick={() => copyToClipboard(exportData.integration_code.router_registration, 'router')} className="text-xs text-blue-600 hover:text-blue-800 flex items-center gap-1">
                          <Copy className="w-3 h-3" /> {copiedCode === 'router' ? 'Copied!' : 'Copy'}
                        </button>
                      </div>
                      <pre className="bg-gray-900 text-green-400 rounded p-3 text-xs overflow-x-auto">{exportData.integration_code.router_registration}</pre>
                    </div>

                    {/* React routes */}
                    <div className="mb-3">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs font-medium text-gray-500">Add to App.tsx (React)</span>
                        <button onClick={() => copyToClipboard(exportData.integration_code.react_routes, 'routes')} className="text-xs text-blue-600 hover:text-blue-800 flex items-center gap-1">
                          <Copy className="w-3 h-3" /> {copiedCode === 'routes' ? 'Copied!' : 'Copy'}
                        </button>
                      </div>
                      <pre className="bg-gray-900 text-green-400 rounded p-3 text-xs overflow-x-auto">{exportData.integration_code.react_routes}</pre>
                    </div>

                    {/* Menu items */}
                    <div className="mb-3">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-xs font-medium text-gray-500">Add to Layout.tsx (Menu)</span>
                        <button onClick={() => copyToClipboard(exportData.integration_code.menu_items, 'menu')} className="text-xs text-blue-600 hover:text-blue-800 flex items-center gap-1">
                          <Copy className="w-3 h-3" /> {copiedCode === 'menu' ? 'Copied!' : 'Copy'}
                        </button>
                      </div>
                      <pre className="bg-gray-900 text-green-400 rounded p-3 text-xs overflow-x-auto">{exportData.integration_code.menu_items}</pre>
                    </div>
                  </div>

                  {/* Settings */}
                  {Object.keys(exportData.settings || {}).length > 0 && (
                    <div>
                      <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2 mb-2">
                        <Settings className="w-4 h-4" /> Settings to Configure
                      </h3>
                      <table className="w-full text-xs">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-3 py-1.5 text-left text-gray-500">Setting</th>
                            <th className="px-3 py-1.5 text-left text-gray-500">Type</th>
                            <th className="px-3 py-1.5 text-left text-gray-500">Current Value</th>
                            <th className="px-3 py-1.5 text-left text-gray-500">Required</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100">
                          {Object.entries(exportData.settings).map(([key, s]: [string, any]) => (
                            <tr key={key}>
                              <td className="px-3 py-1.5 font-medium text-gray-700">{s.label}<br /><span className="text-gray-400 font-mono">{key}</span></td>
                              <td className="px-3 py-1.5 text-gray-500">{s.type}</td>
                              <td className="px-3 py-1.5 text-gray-600 font-mono">{s.type === 'secret' ? '••••••' : (s.value || s.default || '-')}</td>
                              <td className="px-3 py-1.5">{s.required ? <span className="text-red-600">Yes</span> : <span className="text-gray-400">No</span>}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}

                  {/* Data Files */}
                  {exportData.data_files?.length > 0 && (
                    <div>
                      <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2 mb-2">
                        <Database className="w-4 h-4" /> Data Files to Migrate
                      </h3>
                      <div className="space-y-1">
                        {exportData.data_files.map((d: any, i: number) => (
                          <div key={i} className="flex items-center justify-between p-2 bg-gray-50 rounded text-xs">
                            <div>
                              <span className="font-mono text-gray-700">{d.file}</span>
                              <span className="text-gray-400 ml-2">{d.description}</span>
                            </div>
                            <span className="text-gray-400">{d.size_formatted}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Locking Rules */}
                  {Object.keys(exportData.locking_rules || {}).length > 0 && (
                    <div>
                      <h3 className="text-sm font-semibold text-gray-700 flex items-center gap-2 mb-2">
                        <Lock className="w-4 h-4" /> Locking Rules
                      </h3>
                      <div className="space-y-1">
                        {Object.entries(exportData.locking_rules).map(([key, desc]: [string, any]) => (
                          <div key={key} className="text-xs p-2 bg-red-50 rounded">
                            <span className="font-medium text-red-800">{key.replace(/_/g, ' ')}</span>: <span className="text-red-700">{desc}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Dependencies */}
                  {Object.keys(exportData.dependencies || {}).length > 0 && (
                    <div>
                      <h3 className="text-sm font-semibold text-gray-700 mb-2">Dependencies</h3>
                      <div className="space-y-1">
                        {Object.entries(exportData.dependencies).map(([key, dep]: [string, any]) => (
                          <div key={key} className="flex items-center justify-between p-2 bg-blue-50 rounded text-xs">
                            <span className="font-medium text-blue-800">{key}</span>
                            <span className="text-blue-600">{dep.type} — {dep.access || dep.auth || ''}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                </div>
              )}
            </Card>
          );
        })}
      </div>
    </div>
  );
}
