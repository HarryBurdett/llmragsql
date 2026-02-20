import type { ReactNode } from 'react';

type BadgeVariant = 'info' | 'success' | 'warning' | 'danger' | 'neutral';

const variantClasses: Record<BadgeVariant, string> = {
  info: 'bg-blue-50 text-blue-700',
  success: 'bg-emerald-50 text-emerald-700',
  warning: 'bg-amber-50 text-amber-700',
  danger: 'bg-red-50 text-red-700',
  neutral: 'bg-gray-100 text-gray-600',
};

interface StatusBadgeProps {
  variant?: BadgeVariant;
  children: ReactNode;
}

export function StatusBadge({ variant = 'info', children }: StatusBadgeProps) {
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 text-xs font-medium rounded-full ${variantClasses[variant]}`}>
      {children}
    </span>
  );
}
