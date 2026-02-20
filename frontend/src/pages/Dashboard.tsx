import { useQuery } from '@tanstack/react-query';
import { Database, Brain, Server, CheckCircle, XCircle, LayoutDashboard } from 'lucide-react';
import apiClient from '../api/client';
import { PageHeader, Card, LoadingState } from '../components/ui';

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
    <div className="flex items-center gap-2">
      {active ? (
        <CheckCircle className="h-4 w-4 text-emerald-500" />
      ) : (
        <XCircle className="h-4 w-4 text-red-400" />
      )}
      <span className={`text-sm ${active ? 'text-emerald-700' : 'text-red-600'}`}>{label}</span>
    </div>
  );

  return (
    <div className="space-y-6">
      <PageHeader icon={LayoutDashboard} title="Dashboard" subtitle="System status overview" />

      {isLoading ? (
        <LoadingState message="Loading status..." />
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card>
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 bg-blue-50 rounded-xl flex items-center justify-center">
                <Database className="h-5 w-5 text-blue-600" />
              </div>
              <h3 className="text-base font-semibold text-gray-900">Database</h3>
            </div>
            <StatusIndicator
              active={statusData?.sql_connector || false}
              label={statusData?.sql_connector ? 'Connected' : 'Not Connected'}
            />
          </Card>

          <Card>
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 bg-purple-50 rounded-xl flex items-center justify-center">
                <Server className="h-5 w-5 text-purple-600" />
              </div>
              <h3 className="text-base font-semibold text-gray-900">Vector Database</h3>
            </div>
            <StatusIndicator
              active={statusData?.vector_db || false}
              label={statusData?.vector_db ? 'Connected' : 'Not Connected'}
            />
            {stats?.stats && (
              <p className="text-xs text-gray-500 mt-2">
                {stats.stats.vectors_count || 0} vectors
              </p>
            )}
          </Card>

          <Card>
            <div className="flex items-center gap-3 mb-4">
              <div className="w-10 h-10 bg-emerald-50 rounded-xl flex items-center justify-center">
                <Brain className="h-5 w-5 text-emerald-600" />
              </div>
              <h3 className="text-base font-semibold text-gray-900">LLM</h3>
            </div>
            <StatusIndicator
              active={statusData?.llm || false}
              label={statusData?.llm ? 'Initialized' : 'Not Initialized'}
            />
          </Card>
        </div>
      )}

      <Card title="Quick Actions">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <a
            href="/database"
            className="flex items-center gap-3 p-4 bg-gray-50 rounded-xl hover:bg-blue-50 transition-colors group"
          >
            <div className="w-10 h-10 bg-blue-100 rounded-xl flex items-center justify-center group-hover:bg-blue-200 transition-colors">
              <Database className="h-5 w-5 text-blue-600" />
            </div>
            <div>
              <p className="text-sm font-semibold text-gray-900">Query Database</p>
              <p className="text-xs text-gray-500">Execute SQL queries</p>
            </div>
          </a>
          <a
            href="/ask"
            className="flex items-center gap-3 p-4 bg-gray-50 rounded-xl hover:bg-emerald-50 transition-colors group"
          >
            <div className="w-10 h-10 bg-emerald-100 rounded-xl flex items-center justify-center group-hover:bg-emerald-200 transition-colors">
              <Brain className="h-5 w-5 text-emerald-600" />
            </div>
            <div>
              <p className="text-sm font-semibold text-gray-900">Ask Questions</p>
              <p className="text-xs text-gray-500">Natural language queries</p>
            </div>
          </a>
          <a
            href="/settings"
            className="flex items-center gap-3 p-4 bg-gray-50 rounded-xl hover:bg-purple-50 transition-colors group"
          >
            <div className="w-10 h-10 bg-purple-100 rounded-xl flex items-center justify-center group-hover:bg-purple-200 transition-colors">
              <Server className="h-5 w-5 text-purple-600" />
            </div>
            <div>
              <p className="text-sm font-semibold text-gray-900">Settings</p>
              <p className="text-xs text-gray-500">Configure LLM & Database</p>
            </div>
          </a>
        </div>
      </Card>
    </div>
  );
}
