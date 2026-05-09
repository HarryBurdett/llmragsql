import { RefreshCw } from 'lucide-react';

interface LoadingStateProps {
  message?: string;
  size?: 'sm' | 'md' | 'lg';
}

const sizeClasses = {
  sm: { icon: 'h-4 w-4', text: 'text-sm', padding: 'py-4' },
  md: { icon: 'h-6 w-6', text: 'text-sm', padding: 'py-12' },
  lg: { icon: 'h-8 w-8', text: 'text-base', padding: 'py-20' },
};

export function LoadingState({ message, size = 'md' }: LoadingStateProps) {
  const s = sizeClasses[size];

  return (
    <div className={`flex flex-col items-center justify-center ${s.padding}`}>
      <RefreshCw className={`${s.icon} animate-spin text-blue-500 mb-3`} />
      {message && <p className={`${s.text} text-gray-500`}>{message}</p>}
    </div>
  );
}
