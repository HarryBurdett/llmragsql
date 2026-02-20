import { ChevronDown, ChevronRight } from 'lucide-react';
import { StatusBadge } from './StatusBadge';

interface SectionHeaderProps {
  title: string;
  icon: React.ComponentType<{ className?: string }>;
  badge?: string | number;
  badgeVariant?: 'info' | 'success' | 'warning' | 'danger' | 'neutral';
  expanded: boolean;
  onToggle: () => void;
}

export function SectionHeader({ title, icon: Icon, badge, badgeVariant = 'info', expanded, onToggle }: SectionHeaderProps) {
  return (
    <button
      onClick={onToggle}
      className="w-full flex items-center justify-between p-4 bg-gray-50 hover:bg-gray-100 rounded-xl transition-colors"
    >
      <div className="flex items-center gap-3">
        <div className="p-2 bg-white rounded-lg shadow-sm">
          <Icon className="h-5 w-5 text-blue-600" />
        </div>
        <span className="font-semibold text-gray-900">{title}</span>
        {badge !== undefined && <StatusBadge variant={badgeVariant}>{badge}</StatusBadge>}
      </div>
      {expanded ? (
        <ChevronDown className="h-5 w-5 text-gray-400" />
      ) : (
        <ChevronRight className="h-5 w-5 text-gray-400" />
      )}
    </button>
  );
}
