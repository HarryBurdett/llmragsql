import type { ReactNode } from 'react';

interface EmptyStateProps {
  icon: React.ComponentType<{ className?: string }>;
  title: string;
  message?: string;
  children?: ReactNode;
}

export function EmptyState({ icon: Icon, title, message, children }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center">
      <div className="w-14 h-14 bg-gray-100 rounded-2xl flex items-center justify-center mb-4">
        <Icon className="h-7 w-7 text-gray-400" />
      </div>
      <h3 className="text-base font-semibold text-gray-900 mb-1">{title}</h3>
      {message && <p className="text-sm text-gray-500 max-w-sm">{message}</p>}
      {children && <div className="mt-4">{children}</div>}
    </div>
  );
}
