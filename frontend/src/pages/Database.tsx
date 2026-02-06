import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Play, Database as DbIcon, Table, Download, Upload, Sparkles, Trash2 } from 'lucide-react';
import apiClient from '../api/client';
import type { SQLQueryResponse, SQLToRAGResponse } from '../api/client';

export function Database() {
  const queryClient = useQueryClient();
  const [query, setQuery] = useState('SELECT TOP 100 * FROM');
  const [storeInVector, setStoreInVector] = useState(false);
  const [result, setResult] = useState<SQLQueryResponse | null>(null);
  const [selectedTable, setSelectedTable] = useState<string | null>(null);

  // AI Ingestion state
  const [aiDescription, setAiDescription] = useState('');
  const [aiMaxRows, setAiMaxRows] = useState(500);
  const [aiResult, setAiResult] = useState<SQLToRAGResponse | null>(null);

  // Get tables
  const { data: tables, isLoading: tablesLoading } = useQuery({
    queryKey: ['tables'],
    queryFn: () => apiClient.getTables(),
  });

  // Get columns for selected table
  const { data: columns } = useQuery({
    queryKey: ['columns', selectedTable],
    queryFn: () => apiClient.getColumns(selectedTable!),
    enabled: !!selectedTable,
  });

  // Execute query mutation
  const queryMutation = useMutation({
    mutationFn: ({ sql, store }: { sql: string; store: boolean }) =>
      apiClient.executeQuery(sql, store),
    onSuccess: (response) => {
      setResult(response.data);
    },
  });

  // Generate SQL mutation
  const generateMutation = useMutation({
    mutationFn: (question: string) => apiClient.generateSQL(question),
    onSuccess: (response) => {
      if (response.data.success && response.data.sql) {
        setQuery(response.data.sql);
      }
    },
  });

  // AI SQL-to-RAG ingestion mutation
  const aiIngestMutation = useMutation({
    mutationFn: (description: string) =>
      apiClient.ingestFromSQL({ description, max_rows: aiMaxRows }),
    onSuccess: (response) => {
      setAiResult(response.data);
      queryClient.invalidateQueries({ queryKey: ['vectorStats'] });
    },
  });

  // Clear vector DB mutation
  const clearVectorMutation = useMutation({
    mutationFn: () => apiClient.clearVectorDB(),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['vectorStats'] });
    },
  });

  // Get vector stats
  const { data: vectorStats } = useQuery({
    queryKey: ['vectorStats'],
    queryFn: () => apiClient.getVectorStats(),
  });

  const handleExecute = () => {
    queryMutation.mutate({ sql: query, store: storeInVector });
  };

  const handleTableClick = (tableName: string) => {
    setSelectedTable(tableName);
    setQuery(`SELECT TOP 100 * FROM ${tableName}`);
  };

  const handleGenerateSQL = () => {
    const question = prompt('Describe what data you want to query:');
    if (question) {
      generateMutation.mutate(question);
    }
  };

  const handleAiIngest = () => {
    aiIngestMutation.mutate(aiDescription);
  };

  const handleClearVectorDB = () => {
    if (confirm('Are you sure you want to clear all data from the vector database?')) {
      clearVectorMutation.mutate();
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Database Explorer</h2>
        <p className="text-gray-600 mt-1">Execute SQL queries and explore your database</p>
      </div>

      {/* AI-Powered SQL to RAG Ingestion */}
      <div className="card bg-gradient-to-r from-purple-50 to-blue-50 border-purple-200">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold flex items-center text-purple-800">
            <Sparkles className="h-5 w-5 mr-2 text-purple-600" />
            AI-Powered Data Ingestion
          </h3>
          <div className="flex items-center gap-4">
            <span className="text-sm text-gray-600">
              RAG Documents: <strong>{vectorStats?.data?.stats?.document_count ?? 0}</strong>
            </span>
            <button
              onClick={handleClearVectorDB}
              disabled={clearVectorMutation.isPending}
              className="btn btn-secondary text-sm flex items-center text-red-600 hover:text-red-700"
            >
              <Trash2 className="h-4 w-4 mr-1" />
              Clear
            </button>
          </div>
        </div>
        <p className="text-sm text-gray-600 mb-4">
          Describe what data you want to extract from MS SQL. AI will generate the query, execute it, and store results in the RAG database.
        </p>
        <div className="flex gap-4">
          <div className="flex-1">
            <textarea
              value={aiDescription}
              onChange={(e) => setAiDescription(e.target.value)}
              className="input text-sm h-20 resize-none"
              placeholder="Example: Get all customers with their recent orders and contact information..."
            />
          </div>
          <div className="flex flex-col gap-2">
            <label className="text-sm text-gray-600">
              Max rows:
              <input
                type="number"
                value={aiMaxRows}
                onChange={(e) => setAiMaxRows(Number(e.target.value))}
                className="input w-24 ml-2 text-sm"
                min={1}
                max={10000}
              />
            </label>
            <button
              onClick={handleAiIngest}
              disabled={aiIngestMutation.isPending}
              className="btn btn-primary flex items-center justify-center"
            >
              <Upload className="h-4 w-4 mr-2" />
              {aiIngestMutation.isPending ? 'Processing...' : 'Ingest to RAG'}
            </button>
          </div>
        </div>

        {/* AI Ingestion Result */}
        {aiResult && (
          <div className={`mt-4 p-4 rounded-md ${aiResult.success ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'}`}>
            {aiResult.success ? (
              <div>
                <p className="text-green-800 font-medium">{aiResult.message}</p>
                {aiResult.sql_used && (
                  <details className="mt-2">
                    <summary className="text-sm text-green-700 cursor-pointer">View generated SQL</summary>
                    <pre className="mt-2 p-2 bg-white rounded text-xs overflow-x-auto">{aiResult.sql_used}</pre>
                  </details>
                )}
                {aiResult.sample_data && aiResult.sample_data.length > 0 && (
                  <details className="mt-2">
                    <summary className="text-sm text-green-700 cursor-pointer">View sample data ({aiResult.sample_data.length} rows)</summary>
                    <pre className="mt-2 p-2 bg-white rounded text-xs overflow-x-auto">
                      {JSON.stringify(aiResult.sample_data, null, 2)}
                    </pre>
                  </details>
                )}
              </div>
            ) : (
              <p className="text-red-800"><strong>Error:</strong> {aiResult.error}</p>
            )}
          </div>
        )}
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* Sidebar - Tables */}
        <div className="card lg:col-span-1">
          <h3 className="text-lg font-semibold mb-4 flex items-center">
            <Table className="h-5 w-5 mr-2" />
            Tables
          </h3>
          {tablesLoading ? (
            <p className="text-gray-500">Loading tables...</p>
          ) : tables?.data && tables.data.length > 0 ? (
            <ul className="space-y-1">
              {tables.data.map((table) => (
                <li key={`${table.schema_name}.${table.table_name}`}>
                  <button
                    onClick={() => handleTableClick(table.table_name)}
                    className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors ${
                      selectedTable === table.table_name
                        ? 'bg-blue-100 text-blue-700'
                        : 'hover:bg-gray-100 text-gray-700'
                    }`}
                  >
                    <DbIcon className="h-4 w-4 inline mr-2" />
                    {table.table_name}
                  </button>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-gray-500 text-sm">No tables found</p>
          )}

          {/* Selected table columns */}
          {selectedTable && columns?.data && (
            <div className="mt-4 pt-4 border-t border-gray-200">
              <h4 className="text-sm font-semibold text-gray-700 mb-2">
                Columns in {selectedTable}
              </h4>
              <ul className="space-y-1">
                {columns.data.map((col) => (
                  <li
                    key={col.column_name}
                    className="text-xs text-gray-600 flex justify-between"
                  >
                    <span>{col.column_name}</span>
                    <span className="text-gray-400">{col.data_type}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        {/* Main Content - Query Editor & Results */}
        <div className="lg:col-span-3 space-y-4">
          {/* Query Editor */}
          <div className="card">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold">SQL Query</h3>
              <button
                onClick={handleGenerateSQL}
                disabled={generateMutation.isPending}
                className="btn btn-secondary text-sm flex items-center"
              >
                {generateMutation.isPending ? 'Generating...' : 'Generate from question'}
              </button>
            </div>
            <textarea
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="input font-mono text-sm h-32 resize-y"
              placeholder="Enter your SQL query here..."
            />
            <div className="flex items-center justify-between mt-4">
              <label className="flex items-center text-sm text-gray-600">
                <input
                  type="checkbox"
                  checked={storeInVector}
                  onChange={(e) => setStoreInVector(e.target.checked)}
                  className="mr-2"
                />
                <Upload className="h-4 w-4 mr-1" />
                Store results in vector database
              </label>
              <button
                onClick={handleExecute}
                disabled={queryMutation.isPending}
                className="btn btn-primary flex items-center"
              >
                <Play className="h-4 w-4 mr-2" />
                {queryMutation.isPending ? 'Executing...' : 'Execute Query'}
              </button>
            </div>
          </div>

          {/* Results */}
          {result && (
            <div className="card">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold">
                  Results {result.success && `(${result.row_count} rows)`}
                </h3>
                {result.success && result.data.length > 0 && (
                  <button className="btn btn-secondary text-sm flex items-center">
                    <Download className="h-4 w-4 mr-1" />
                    Export CSV
                  </button>
                )}
              </div>

              {result.error ? (
                <div className="bg-red-50 text-red-700 p-4 rounded-md">
                  <strong>Error:</strong> {result.error}
                </div>
              ) : result.data.length === 0 ? (
                <div className="text-gray-500 text-center py-8">
                  No results returned
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="data-table">
                    <thead>
                      <tr>
                        {result.columns.map((col) => (
                          <th key={col}>{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.data.slice(0, 100).map((row, idx) => (
                        <tr key={idx}>
                          {result.columns.map((col) => (
                            <td key={col}>
                              {row[col] !== null && row[col] !== undefined
                                ? String(row[col]).substring(0, 100)
                                : <span className="text-gray-400">NULL</span>}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {result.data.length > 100 && (
                    <p className="text-sm text-gray-500 mt-2 text-center">
                      Showing first 100 of {result.row_count} rows
                    </p>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
