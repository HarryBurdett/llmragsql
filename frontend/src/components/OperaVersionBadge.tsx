import { useQuery } from '@tanstack/react-query';
import { authFetch } from '../api/client';

interface OperaConfig {
  version: string;
  opera3_base_path?: string;
}

export function OperaVersionBadge() {
  const { data, isLoading } = useQuery<OperaConfig>({
    queryKey: ['operaConfig'],
    queryFn: async () => {
      const response = await authFetch('/api/config/opera');
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
      className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-xs font-semibold ${
        isOpera3
          ? 'bg-amber-50 text-amber-700 border border-amber-300'
          : 'bg-emerald-50 text-emerald-700 border border-emerald-300'
      }`}
      title={isOpera3 ? 'Connected to Opera 3 (FoxPro DBF files)' : 'Connected to Opera SQL SE (SQL Server)'}
    >
      {isOpera3 ? (
        <img src="/opera3-logo.png" alt="Opera 3" className="h-8" />
      ) : (
        <img src="/opera-se-logo.png" alt="Opera SQL SE" className="h-8" />
      )}
      <span>{isOpera3 ? 'Opera 3' : 'Opera SQL SE'}</span>
    </div>
  );
}

export default OperaVersionBadge;
