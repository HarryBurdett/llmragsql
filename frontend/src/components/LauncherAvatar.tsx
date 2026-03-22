import { Bot } from 'lucide-react';

export function LauncherAvatar() {
  return (
    <div className="flex flex-col items-center gap-3">
      <div className="relative">
        {/* Animated gradient ring */}
        <div className="absolute -inset-1 rounded-full bg-gradient-to-r from-blue-500 via-purple-500 to-emerald-500 opacity-40 blur-sm animate-pulse" />
        <div className="relative w-24 h-24 rounded-full bg-white/10 backdrop-blur-sm border border-white/20 flex items-center justify-center">
          <Bot className="h-10 w-10 text-white/60" />
        </div>
      </div>
      <div className="text-center">
        <div className="text-xs text-white/30 uppercase tracking-widest">AI Assistant</div>
        <div className="text-xs text-white/20 mt-0.5">Coming Soon</div>
      </div>
    </div>
  );
}
