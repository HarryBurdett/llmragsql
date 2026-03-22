import { useNavigate } from 'react-router-dom';
import {
  CreditCard, Truck, Landmark, LayoutDashboard,
  FileText, Receipt, Scale,
} from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { LauncherTile } from '../components/LauncherTile';
import { LauncherAvatar } from '../components/LauncherAvatar';

const PRODUCTS = [
  { id: 'dd',  title: 'Crakd.ai DD',  subtitle: 'Direct Debits',       icon: CreditCard,     color: 'emerald', route: '/cashbook/gocardless',       permission: 'cashbook',       voiceAliases: ['direct debits', 'dd', 'gocardless'] },
  { id: 'sm',  title: 'Crakd.ai SM',  subtitle: 'Supplier Management', icon: Truck,           color: 'amber',   route: '/supplier/dashboard',        permission: 'ap_automation',  voiceAliases: ['supplier management', 'suppliers'] },
  { id: 'br',  title: 'Crakd.ai BR',  subtitle: 'Bank Reconciliation', icon: Landmark,        color: 'blue',    route: '/cashbook/bank-hub',         permission: 'cashbook',       voiceAliases: ['bank reconciliation', 'bank rec'] },
  { id: 'db',  title: 'Crakd.ai DB',  subtitle: 'Dashboard',           icon: LayoutDashboard, color: 'indigo',  route: '/archive/dashboard',         permission: null,             voiceAliases: ['dashboard'] },
  { id: 'pm',  title: 'Crakd.ai PM',  subtitle: 'Purchase Management', icon: FileText,        color: 'rose',    route: '/supplier/statements/queue', permission: 'ap_automation',  voiceAliases: ['purchase management', 'purchases'] },
  { id: 'exp', title: 'Crakd.ai EXP', subtitle: 'Expenses',            icon: Receipt,         color: 'orange',  route: '/expenses',                  permission: 'cashbook',       voiceAliases: ['expenses'] },
  { id: 'rec', title: 'Crakd.ai Rec', subtitle: 'Reconciliation',      icon: Scale,           color: 'purple',  route: '/reconcile/summary',         permission: 'administration', voiceAliases: ['reconciliation', 'balance check'] },
] as const;

function getGreeting(): string {
  const hour = new Date().getHours();
  if (hour < 12) return 'Good morning';
  if (hour < 17) return 'Good afternoon';
  return 'Good evening';
}

export function LauncherHome() {
  const { user, hasPermission } = useAuth();
  const navigate = useNavigate();

  const visibleProducts = PRODUCTS.filter(
    (p) => p.permission === null || hasPermission(p.permission)
  );

  return (
    <div className="fixed inset-0 bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 flex flex-col overflow-auto" style={{ zIndex: 35 }}>
      {/* Header */}
      <div className="pt-8 pb-4 text-center">
        <h1 className="text-2xl font-bold text-white/90">
          {getGreeting()}{user?.display_name ? `, ${user.display_name.split(' ')[0]}` : ''}
        </h1>
        <p className="text-sm text-white/40 mt-1">crakd.ai — Automating the Accounting Function</p>
      </div>

      {/* Avatar */}
      <div className="flex justify-center py-6">
        <LauncherAvatar />
      </div>

      {/* Product Grid */}
      <div className="flex-1 flex items-start justify-center px-6 pb-12">
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4 max-w-3xl w-full">
          {visibleProducts.map((product) => (
            <LauncherTile
              key={product.id}
              title={product.title}
              subtitle={product.subtitle}
              icon={product.icon}
              color={product.color}
              onClick={() => navigate(product.route)}
            />
          ))}
        </div>
      </div>

      {/* Footer */}
      <div className="py-4 text-center">
        <button
          onClick={() => navigate('/?view=classic')}
          className="text-xs text-white/20 hover:text-white/40 transition-colors"
        >
          Switch to Classic View
        </button>
      </div>
    </div>
  );
}
