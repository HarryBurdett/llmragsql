import { Link, useSearchParams } from 'react-router-dom';
import {
  Landmark, CreditCard,
  Truck, Send, FileText, Scale, MessageSquare, Receipt,
} from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { LauncherHome } from './LauncherHome';

export function Home() {
  const { user, hasPermission } = useAuth();
  const [searchParams] = useSearchParams();

  // Show launcher if user preference is 'launcher' (unless ?view=classic override)
  const viewOverride = searchParams.get('view');
  if (user?.ui_mode === 'launcher' && viewOverride !== 'classic') {
    return <LauncherHome />;
  }

  // Build grouped action sections — each group has its own accent colour
  const groups: { label: string; color: string; items: { to: string; label: string; icon: React.ComponentType<{ className?: string }> }[] }[] = [];
  if (hasPermission('cashbook')) {
    groups.push({
      label: 'Cashbook',
      color: 'blue',
      items: [
        { to: '/cashbook/bank-hub', label: 'Bank Statements', icon: Landmark },
        { to: '/cashbook/gocardless', label: 'GoCardless Import', icon: CreditCard },
        { to: '/cashbook/gocardless-requests', label: 'GoCardless Requests', icon: Send },
      ],
    });
  }
  if (hasPermission('ap_automation')) {
    groups.push({
      label: 'Suppliers',
      color: 'amber',
      items: [
        { to: '/supplier/dashboard', label: 'Dashboard', icon: Truck },
        { to: '/supplier/statements/queue', label: 'Statement Queue', icon: FileText },
        { to: '/supplier/statements/reconciliations', label: 'Reconciliations', icon: Scale },
        { to: '/supplier/account', label: 'Account Lookup', icon: Receipt },
        { to: '/supplier/queries/open', label: 'Open Queries', icon: MessageSquare },
      ],
    });
  }
  if (hasPermission('payroll')) {
    groups.push({
      label: 'Payroll',
      color: 'emerald',
      items: [
        { to: '/payroll/pension-export', label: 'Pension Export', icon: FileText },
      ],
    });
  }

  return (
    <div className="space-y-6">
      {/* Welcome */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">
          {getGreeting()}{user?.display_name ? `, ${user.display_name.split(' ')[0]}` : ''}
        </h1>
      </div>

      {/* Grouped action grid */}
      {groups.map((group) => {
        const gc = groupColors[group.color] || groupColors.blue;
        return (
          <div key={group.label}>
            <h2 className={`text-xs font-semibold uppercase tracking-wider mb-2 ${gc.label}`}>{group.label}</h2>
            <div className="grid grid-cols-3 sm:grid-cols-4 lg:grid-cols-5 gap-3">
              {group.items.map(({ to, label, icon: Icon }) => (
                <Link
                  key={to}
                  to={to}
                  className={`group flex flex-col items-center gap-2 p-4 rounded-xl bg-white border hover:shadow-sm transition-all text-center ${gc.border} ${gc.hoverBorder}`}
                >
                  <div className={`p-2.5 rounded-lg ${gc.iconBg} transition-colors`}>
                    <Icon className={`h-5 w-5 ${gc.icon} transition-colors`} />
                  </div>
                  <span className="text-xs font-medium text-gray-600 group-hover:text-gray-900 leading-tight transition-colors">{label}</span>
                </Link>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function getGreeting(): string {
  const hour = new Date().getHours();
  if (hour < 12) return 'Good morning';
  if (hour < 17) return 'Good afternoon';
  return 'Good evening';
}

const groupColors: Record<string, { label: string; border: string; hoverBorder: string; iconBg: string; icon: string }> = {
  blue:    { label: 'text-blue-400',    border: 'border-blue-100',    hoverBorder: 'hover:border-blue-300',    iconBg: 'bg-blue-50 group-hover:bg-blue-100',    icon: 'text-blue-500 group-hover:text-blue-600' },
  amber:   { label: 'text-amber-400',   border: 'border-amber-100',   hoverBorder: 'hover:border-amber-300',   iconBg: 'bg-amber-50 group-hover:bg-amber-100',   icon: 'text-amber-500 group-hover:text-amber-600' },
  emerald: { label: 'text-emerald-400', border: 'border-emerald-100', hoverBorder: 'hover:border-emerald-300', iconBg: 'bg-emerald-50 group-hover:bg-emerald-100', icon: 'text-emerald-500 group-hover:text-emerald-600' },
  purple:  { label: 'text-purple-400',  border: 'border-purple-100',  hoverBorder: 'hover:border-purple-300',  iconBg: 'bg-purple-50 group-hover:bg-purple-100',  icon: 'text-purple-500 group-hover:text-purple-600' },
};

export default Home;
