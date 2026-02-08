import { useQuery } from '@tanstack/react-query';
import { Database, Server } from 'lucide-react';

interface OperaConfig {
  version: string;
  opera3_base_path?: string;
}

export function OperaVersionBadge() {
  const { data, isLoading } = useQuery<OperaConfig>({
    queryKey: ['operaConfig'],
    queryFn: async () => {
      const response = await fetch('/api/config/opera');
      return response.json();
    },
    staleTime: 60000, // Cache for 1 minute
  });

  if (isLoading) {
    return null;
  }

  const version = data?.version || 'sql_se';
  const isOpera3 = version === 'opera3';

  return (
    <div
      className={`flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${
        isOpera3
          ? 'bg-amber-100 text-amber-800 border border-amber-200'
          : 'bg-blue-100 text-blue-800 border border-blue-200'
      }`}
      title={isOpera3 ? 'Connected to Opera 3 (FoxPro)' : 'Connected to Opera SQL SE'}
    >
      {isOpera3 ? (
        <Database className="h-3 w-3" />
      ) : (
        <Server className="h-3 w-3" />
      )}
      <span>{isOpera3 ? 'Opera 3' : 'Opera SQL SE'}</span>
    </div>
  );
}

export default OperaVersionBadge;
