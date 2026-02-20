import type { ReactNode } from 'react';

interface CardProps {
  title?: string;
  icon?: React.ComponentType<{ className?: string }>;
  children: ReactNode;
  className?: string;
  padding?: boolean;
}

export function Card({ title, icon: Icon, children, className = '', padding = true }: CardProps) {
  return (
    <div className={`bg-white rounded-xl border border-gray-200 shadow-sm ${className}`}>
      {(title || Icon) && (
        <div className="px-6 py-4 border-b border-gray-100">
          <div className="flex items-center gap-2.5">
            {Icon && <Icon className="h-5 w-5 text-gray-400" />}
            {title && <h3 className="text-base font-semibold text-gray-900">{title}</h3>}
          </div>
        </div>
      )}
      <div className={padding ? 'p-6' : ''}>{children}</div>
    </div>
  );
}
