import { Link } from 'react-router-dom';
import {
  Activity,
  BarChart3,
  CreditCard,
  Wallet,
  TrendingUp,
  FileText,
  Building,
  Scale,
  Upload,
  Mail,
  MessageSquare
} from 'lucide-react';

const archiveItems = [
  {
    category: 'Overview & Analytics',
    items: [
      { path: '/archive/dashboard', label: 'Dashboard', icon: Activity, description: 'Main dashboard with key metrics' },
      { path: '/archive/sales-dashboards', label: 'Sales Dashboards', icon: BarChart3, description: 'Sales performance and analytics' },
    ]
  },
  {
    category: 'Finance & Control',
    items: [
      { path: '/archive/debtors-control', label: 'Debtors Control', icon: CreditCard, description: 'Sales ledger and debtor management' },
      { path: '/archive/creditors-control', label: 'Creditors Control', icon: Wallet, description: 'Purchase ledger and creditor management' },
      { path: '/archive/cashflow', label: 'Cashflow', icon: TrendingUp, description: 'Cash flow analysis and forecasting' },
      { path: '/archive/trial-balance', label: 'Trial Balance', icon: FileText, description: 'Trial balance reporting' },
      { path: '/archive/statutory-accounts', label: 'Statutory Accounts', icon: Building, description: 'Statutory financial statements' },
      { path: '/archive/reconcile', label: 'Reconcile', icon: Scale, description: 'Ledger reconciliation tools' },
      { path: '/archive/imports', label: 'Imports', icon: Upload, description: 'Data import utilities' },
    ]
  },
  {
    category: 'Communication & AI',
    items: [
      { path: '/archive/email', label: 'Email', icon: Mail, description: 'Email management and templates' },
      { path: '/archive/ask', label: 'AI Assistant', icon: MessageSquare, description: 'Natural language queries' },
    ]
  }
];

export function Archive() {
  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Archive</h1>
        <p className="mt-1 text-sm text-gray-500">
          Access previous features and tools
        </p>
      </div>

      {archiveItems.map((section) => (
        <div key={section.category}>
          <h2 className="text-lg font-semibold text-gray-700 mb-4">{section.category}</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {section.items.map((item) => {
              const Icon = item.icon;
              return (
                <Link
                  key={item.path}
                  to={item.path}
                  className="bg-white rounded-lg shadow-sm border border-gray-200 p-4 hover:shadow-md hover:border-blue-300 transition-all"
                >
                  <div className="flex items-start gap-3">
                    <div className="p-2 bg-blue-50 rounded-lg">
                      <Icon className="h-5 w-5 text-blue-600" />
                    </div>
                    <div>
                      <h3 className="font-medium text-gray-900">{item.label}</h3>
                      <p className="text-sm text-gray-500 mt-1">{item.description}</p>
                    </div>
                  </div>
                </Link>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}
