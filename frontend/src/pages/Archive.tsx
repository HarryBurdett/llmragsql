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
  MessageSquare,
  Archive as ArchiveIcon,
} from 'lucide-react';
import { PageHeader, Card, SectionHeader } from '../components/ui';
import { useState } from 'react';

const archiveItems = [
  {
    category: 'Overview & Analytics',
    key: 'overview',
    icon: Activity,
    items: [
      { path: '/archive/dashboard', label: 'Dashboard', icon: Activity, description: 'Main dashboard with key metrics' },
      { path: '/archive/sales-dashboards', label: 'Sales Dashboards', icon: BarChart3, description: 'Sales performance and analytics' },
    ]
  },
  {
    category: 'Finance & Control',
    key: 'finance',
    icon: Wallet,
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
    key: 'comms',
    icon: MessageSquare,
    items: [
      { path: '/archive/email', label: 'Email', icon: Mail, description: 'Email management and templates' },
      { path: '/archive/ask', label: 'AI Assistant', icon: MessageSquare, description: 'Natural language queries' },
    ]
  }
];

export function Archive() {
  const [expandedSections, setExpandedSections] = useState<Set<string>>(
    new Set(archiveItems.map(s => s.key))
  );

  const toggleSection = (key: string) => {
    setExpandedSections(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  return (
    <div className="space-y-6">
      <PageHeader
        icon={ArchiveIcon}
        title="Archive"
        subtitle="Access previous features and tools"
      />

      {archiveItems.map((section) => (
        <div key={section.key} className="space-y-3">
          <SectionHeader
            title={section.category}
            icon={section.icon}
            badge={section.items.length}
            badgeVariant="neutral"
            expanded={expandedSections.has(section.key)}
            onToggle={() => toggleSection(section.key)}
          />
          {expandedSections.has(section.key) && (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {section.items.map((item) => {
                const Icon = item.icon;
                return (
                  <Link
                    key={item.path}
                    to={item.path}
                    className="bg-white rounded-xl border border-gray-200 shadow-sm p-4 hover:shadow-md hover:border-blue-300 transition-all"
                  >
                    <div className="flex items-start gap-3">
                      <div className="p-2 bg-blue-50 rounded-lg">
                        <Icon className="h-5 w-5 text-blue-600" />
                      </div>
                      <div>
                        <h3 className="text-sm font-medium text-gray-900">{item.label}</h3>
                        <p className="text-xs text-gray-500 mt-1">{item.description}</p>
                      </div>
                    </div>
                  </Link>
                );
              })}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
