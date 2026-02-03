import type { ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Database, MessageSquare, Settings, Activity, CreditCard, TrendingUp, Mail, FileText, Building, BarChart3, Wallet, Scale } from 'lucide-react';
import { CompanySelector } from './CompanySelector';

interface LayoutProps {
  children: ReactNode;
}

const navItems = [
  // Overview & Analytics
  { path: '/', label: 'Dashboard', icon: Activity },
  { path: '/sales-dashboards', label: 'Sales', icon: BarChart3 },
  // Finance & Control
  { path: '/debtors-control', label: 'Debtors', icon: CreditCard },
  { path: '/creditors-control', label: 'Creditors', icon: Wallet },
  { path: '/cashflow', label: 'Cashflow', icon: TrendingUp },
  { path: '/trial-balance', label: 'Trial Balance', icon: FileText },
  { path: '/statutory-accounts', label: 'Accounts', icon: Building },
  { path: '/reconcile', label: 'Reconcile', icon: Scale },
  // Communication & AI
  { path: '/email', label: 'Email', icon: Mail },
  { path: '/ask', label: 'AI Assistant', icon: MessageSquare },
  // Admin (at the end)
  { path: '/settings', label: 'Settings', icon: Settings },
];

export function Layout({ children }: LayoutProps) {
  const location = useLocation();

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center gap-6">
              <div className="flex items-center">
                <Database className="h-8 w-8 text-blue-600" />
                <h1 className="ml-3 text-xl font-bold text-gray-900">SQL RAG</h1>
              </div>
              <CompanySelector />
            </div>
            <nav className="flex space-x-1">
              {navItems.map((item) => {
                const Icon = item.icon;
                const isActive = location.pathname === item.path;
                return (
                  <Link
                    key={item.path}
                    to={item.path}
                    className={`flex items-center px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                      isActive
                        ? 'bg-blue-100 text-blue-700'
                        : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
                    }`}
                  >
                    <Icon className="h-4 w-4 mr-2" />
                    {item.label}
                  </Link>
                );
              })}
            </nav>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {children}
      </main>
    </div>
  );
}
