import { useQuery } from '@tanstack/react-query';
import { Database, Brain, Server, CheckCircle, XCircle } from 'lucide-react';
import apiClient from '../api/client';

export function Dashboard() {
  const { data: status, isLoading } = useQuery({
    queryKey: ['status'],
    queryFn: () => apiClient.status(),
    refetchInterval: 5000,
  });

  const { data: vectorStats } = useQuery({
    queryKey: ['vectorStats'],
    queryFn: () => apiClient.getVectorStats(),
  });

  const statusData = status?.data;
  const stats = vectorStats?.data;

  const StatusIndicator = ({ active, label }: { active: boolean; label: string }) => (
    <div className="flex items-center space-x-2">
      {active ? (
        <CheckCircle className="h-5 w-5 text-green-500" />
      ) : (
        <XCircle className="h-5 w-5 text-red-500" />
      )}
      <span className={active ? 'text-green-700' : 'text-red-700'}>{label}</span>
    </div>
  );

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Dashboard</h2>
        <p className="text-gray-600 mt-1">SQL RAG Application Status</p>
      </div>

      {isLoading ? (
        <div className="text-center py-8">Loading...</div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          {/* Database Status */}
          <div className="card">
            <div className="flex items-center space-x-3 mb-4">
              <Database className="h-8 w-8 text-blue-600" />
              <h3 className="text-lg font-semibold">Database</h3>
            </div>
            <StatusIndicator
              active={statusData?.sql_connector || false}
              label={statusData?.sql_connector ? 'Connected' : 'Not Connected'}
            />
          </div>

          {/* Vector DB Status */}
          <div className="card">
            <div className="flex items-center space-x-3 mb-4">
              <Server className="h-8 w-8 text-purple-600" />
              <h3 className="text-lg font-semibold">Vector Database</h3>
            </div>
            <StatusIndicator
              active={statusData?.vector_db || false}
              label={statusData?.vector_db ? 'Connected' : 'Not Connected'}
            />
            {stats?.stats && (
              <p className="text-sm text-gray-500 mt-2">
                Vectors: {stats.stats.vectors_count || 0}
              </p>
            )}
          </div>

          {/* LLM Status */}
          <div className="card">
            <div className="flex items-center space-x-3 mb-4">
              <Brain className="h-8 w-8 text-green-600" />
              <h3 className="text-lg font-semibold">LLM</h3>
            </div>
            <StatusIndicator
              active={statusData?.llm || false}
              label={statusData?.llm ? 'Initialized' : 'Not Initialized'}
            />
          </div>
        </div>
      )}

      {/* Quick Actions */}
      <div className="card">
        <h3 className="text-lg font-semibold mb-4">Quick Actions</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <a
            href="/database"
            className="flex items-center p-4 bg-blue-50 rounded-lg hover:bg-blue-100 transition-colors"
          >
            <Database className="h-6 w-6 text-blue-600 mr-3" />
            <div>
              <p className="font-medium text-blue-900">Query Database</p>
              <p className="text-sm text-blue-600">Execute SQL queries</p>
            </div>
          </a>
          <a
            href="/ask"
            className="flex items-center p-4 bg-green-50 rounded-lg hover:bg-green-100 transition-colors"
          >
            <Brain className="h-6 w-6 text-green-600 mr-3" />
            <div>
              <p className="font-medium text-green-900">Ask Questions</p>
              <p className="text-sm text-green-600">Natural language queries</p>
            </div>
          </a>
          <a
            href="/settings"
            className="flex items-center p-4 bg-purple-50 rounded-lg hover:bg-purple-100 transition-colors"
          >
            <Server className="h-6 w-6 text-purple-600 mr-3" />
            <div>
              <p className="font-medium text-purple-900">Settings</p>
              <p className="text-sm text-purple-600">Configure LLM & Database</p>
            </div>
          </a>
        </div>
      </div>
    </div>
  );
}
