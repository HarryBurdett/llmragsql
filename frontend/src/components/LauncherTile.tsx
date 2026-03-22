import type { ComponentType } from 'react';

interface LauncherTileProps {
  title: string;
  subtitle: string;
  icon: ComponentType<{ className?: string }>;
  color: string;
  onClick: () => void;
}

const colorMap: Record<string, { bg: string; icon: string; border: string; glow: string }> = {
  emerald: { bg: 'bg-emerald-500/10', icon: 'text-emerald-400', border: 'border-emerald-500/20 hover:border-emerald-400/50', glow: 'hover:shadow-emerald-500/20' },
  amber:   { bg: 'bg-amber-500/10',   icon: 'text-amber-400',   border: 'border-amber-500/20 hover:border-amber-400/50',   glow: 'hover:shadow-amber-500/20' },
  blue:    { bg: 'bg-blue-500/10',    icon: 'text-blue-400',    border: 'border-blue-500/20 hover:border-blue-400/50',    glow: 'hover:shadow-blue-500/20' },
  indigo:  { bg: 'bg-indigo-500/10',  icon: 'text-indigo-400',  border: 'border-indigo-500/20 hover:border-indigo-400/50',  glow: 'hover:shadow-indigo-500/20' },
  rose:    { bg: 'bg-rose-500/10',    icon: 'text-rose-400',    border: 'border-rose-500/20 hover:border-rose-400/50',    glow: 'hover:shadow-rose-500/20' },
  orange:  { bg: 'bg-orange-500/10',  icon: 'text-orange-400',  border: 'border-orange-500/20 hover:border-orange-400/50',  glow: 'hover:shadow-orange-500/20' },
  purple:  { bg: 'bg-purple-500/10',  icon: 'text-purple-400',  border: 'border-purple-500/20 hover:border-purple-400/50',  glow: 'hover:shadow-purple-500/20' },
};

export function LauncherTile({ title, subtitle, icon: Icon, color, onClick }: LauncherTileProps) {
  const c = colorMap[color] || colorMap.blue;

  return (
    <button
      onClick={onClick}
      className={`group relative flex flex-col items-center gap-3 p-6 rounded-2xl bg-white/5 backdrop-blur-sm border transition-all duration-300 cursor-pointer hover:scale-105 hover:shadow-lg ${c.border} ${c.glow}`}
    >
      <div className={`p-4 rounded-xl ${c.bg} transition-colors`}>
        <Icon className={`h-8 w-8 ${c.icon} transition-colors`} />
      </div>
      <div className="text-center">
        <div className="text-sm font-semibold text-white/90 tracking-wide">{title}</div>
        <div className="text-xs text-white/50 mt-0.5">{subtitle}</div>
      </div>
    </button>
  );
}
