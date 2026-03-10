import { useQuery } from '@tanstack/react-query';
import { authFetch } from '../api/client';

interface AgentStatus {
  available: boolean;
  configured: boolean;
  url?: string;
  message?: string;
  error?: string;
  info?: {
    version?: string;
    uptime?: number;
    data_path?: string;
    harbour_available?: boolean;
    platform?: string;
    hostname?: string;
  };
}

export function Opera3AgentStatus() {
  // Check if we're connected to Opera 3
  const { data: operaConfig } = useQuery({
    queryKey: ['operaConfig'],
    queryFn: async () => {
      const response = await authFetch('/api/config/opera');
      return response.json();
    },
    staleTime: 60000,
  });

  const isOpera3 = operaConfig?.version === 'opera3';

  // Poll agent status (only when Opera 3)
  const { data: status } = useQuery<AgentStatus>({
    queryKey: ['opera3AgentStatus'],
    queryFn: async () => {
      const response = await authFetch('/api/opera3/agent/status');
      return response.json();
    },
    staleTime: 15000,
    refetchInterval: 30000,
    enabled: isOpera3,
  });

  if (!isOpera3) return null;
  if (!status) return null;

  // Not configured
  if (!status.configured) {
    return (
      <div
        className="flex items-center gap-1.5 px-2 py-1 rounded text-xs bg-gray-100 text-gray-500 border border-gray-200"
        title="Opera 3 Write Agent not configured. Set OPERA3_AGENT_URL in environment or company settings."
      >
        <span className="w-2 h-2 rounded-full bg-gray-400" />
        <span>Write Agent</span>
      </div>
    );
  }

  // Online
  if (status.available) {
    const uptime = status.info?.uptime ? formatUptime(status.info.uptime) : '';
    const host = status.info?.hostname || '';
    return (
      <div
        className="flex items-center gap-1.5 px-2 py-1 rounded text-xs bg-green-50 text-green-700 border border-green-200"
        title={`Write Agent online${uptime ? ` (uptime: ${uptime})` : ''}${host ? ` on ${host}` : ''}`}
      >
        <span className="w-2 h-2 rounded-full bg-green-500" />
        <span>Write Agent</span>
      </div>
    );
  }

  // Offline
  return (
    <div
      className="flex items-center gap-1.5 px-2 py-1 rounded text-xs bg-red-50 text-red-700 border border-red-200"
      title={status.message || 'Write Agent offline — Opera 3 imports are disabled'}
    >
      <span className="w-2 h-2 rounded-full bg-red-500" />
      <span>Write Agent</span>
    </div>
  );
}

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  return `${Math.round(seconds / 86400)}d`;
}

export default Opera3AgentStatus;
