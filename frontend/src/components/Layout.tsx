import { useState, useRef, useEffect, type ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Database, Landmark, Settings, Archive, Lock, ChevronDown, ChevronRight, CreditCard, BookOpen, Users, Building2, Scale, Wrench, Truck, FileText, MessageSquare, Shield, LayoutDashboard, Receipt, Briefcase, FolderKanban } from 'lucide-react';
import { CompanySelector } from './CompanySelector';
import { OperaVersionBadge } from './OperaVersionBadge';

interface LayoutProps {
  children: ReactNode;
}

interface NavItem {
  path: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
}

interface NavItemWithSubmenu {
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  submenu: (NavItem | NavItemWithSubmenu)[];
}

type NavEntry = NavItem | NavItemWithSubmenu;

function isNavItemWithSubmenu(item: NavEntry): item is NavItemWithSubmenu {
  return 'submenu' in item;
}

// Check if any path in this item or its children is active
function isItemActive(item: NavEntry, pathname: string): boolean {
  if (isNavItemWithSubmenu(item)) {
    return item.submenu.some(sub => isItemActive(sub, pathname));
  }
  return pathname === item.path || pathname.startsWith(item.path + '/');
}

const cashbookSubmenu: NavItem[] = [
  { path: '/cashbook/bank-rec', label: 'Bank Reconciliation', icon: Landmark },
  { path: '/cashbook/gocardless', label: 'GoCardless Import', icon: CreditCard },
];

const balanceCheckSubmenu: NavItem[] = [
  { path: '/reconcile/summary', label: 'Summary', icon: Scale },
  { path: '/reconcile/trial-balance', label: 'Trial Balance', icon: Database },
  { path: '/reconcile/debtors', label: 'Debtors Balance Check', icon: Users },
  { path: '/reconcile/creditors', label: 'Creditors Balance Check', icon: Building2 },
  { path: '/reconcile/cashbook', label: 'Cashbook Balance Check', icon: BookOpen },
  { path: '/reconcile/vat', label: 'VAT Balance Check', icon: Receipt },
];

const utilitiesSubmenu: (NavItem | NavItemWithSubmenu)[] = [
  { label: 'Balance Check', icon: Scale, submenu: balanceCheckSubmenu },
  { path: '/utilities/user-activity', label: 'User Activity', icon: Users },
];

const supplierStatementsSubmenu: NavItem[] = [
  { path: '/supplier/statements/queue', label: 'Queue', icon: FileText },
  { path: '/supplier/statements/reconciliations', label: 'Reconciliations', icon: Scale },
  { path: '/supplier/statements/history', label: 'History', icon: Archive },
];

const supplierQueriesSubmenu: NavItem[] = [
  { path: '/supplier/queries/open', label: 'Open', icon: MessageSquare },
  { path: '/supplier/queries/overdue', label: 'Overdue', icon: MessageSquare },
  { path: '/supplier/queries/resolved', label: 'Resolved', icon: MessageSquare },
];

const supplierSecuritySubmenu: NavItem[] = [
  { path: '/supplier/security/alerts', label: 'Alerts', icon: Shield },
  { path: '/supplier/security/audit', label: 'Audit Log', icon: FileText },
  { path: '/supplier/security/senders', label: 'Approved Senders', icon: Users },
];

const supplierSubmenu: (NavItem | NavItemWithSubmenu)[] = [
  { path: '/supplier/dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { path: '/supplier/account', label: 'Account', icon: Receipt },
  { label: 'Statements', icon: FileText, submenu: supplierStatementsSubmenu },
  { label: 'Queries', icon: MessageSquare, submenu: supplierQueriesSubmenu },
  { path: '/supplier/communications', label: 'Communications', icon: MessageSquare },
  { path: '/supplier/directory', label: 'Directory', icon: Building2 },
  { label: 'Security', icon: Shield, submenu: supplierSecuritySubmenu },
  { path: '/supplier/settings', label: 'Settings', icon: Settings },
];

const systemSubmenu: NavItem[] = [
  { path: '/system/projects', label: 'Projects', icon: FolderKanban },
  { path: '/system/lock-monitor', label: 'Lock Monitor', icon: Lock },
  { path: '/settings', label: 'Settings', icon: Settings },
];

const payrollSubmenu: NavItem[] = [
  { path: '/payroll/pension-export', label: 'Pension Export', icon: FileText },
  { path: '/payroll/settings', label: 'Parameters', icon: Settings },
];

const navItems: NavEntry[] = [
  { label: 'Cashbook', icon: BookOpen, submenu: cashbookSubmenu },
  { label: 'Payroll', icon: Briefcase, submenu: payrollSubmenu },
  { label: 'AP Automation', icon: Truck, submenu: supplierSubmenu },
  { label: 'Utilities', icon: Wrench, submenu: utilitiesSubmenu },
  { path: '/', label: 'Archive', icon: Archive },
  { label: 'System', icon: Settings, submenu: systemSubmenu },
];

function NestedSubmenu({ item, onClose }: { item: NavItemWithSubmenu; onClose: () => void }) {
  const [isOpen, setIsOpen] = useState(false);
  const location = useLocation();
  const Icon = item.icon;
  const isActive = isItemActive(item, location.pathname);

  return (
    <div
      className="relative"
      onMouseEnter={() => setIsOpen(true)}
      onMouseLeave={() => setIsOpen(false)}
    >
      <button
        className={`w-full flex items-center justify-between px-4 py-2 text-sm transition-colors ${
          isActive
            ? 'bg-blue-50 text-blue-700'
            : 'text-gray-700 hover:bg-gray-100'
        }`}
      >
        <div className="flex items-center">
          <Icon className="h-4 w-4 mr-2" />
          {item.label}
        </div>
        <ChevronRight className="h-4 w-4" />
      </button>
      {isOpen && (
        <div className="absolute left-full top-0 ml-1 w-48 bg-white rounded-md shadow-lg border border-gray-200 py-1 z-50">
          {item.submenu.map((subItem) => {
            if (isNavItemWithSubmenu(subItem)) {
              return <NestedSubmenu key={subItem.label} item={subItem} onClose={onClose} />;
            }
            const SubIcon = subItem.icon;
            const isSubActive = location.pathname === subItem.path;
            return (
              <Link
                key={subItem.path}
                to={subItem.path}
                onClick={onClose}
                className={`flex items-center px-4 py-2 text-sm transition-colors ${
                  isSubActive
                    ? 'bg-blue-50 text-blue-700'
                    : 'text-gray-700 hover:bg-gray-100'
                }`}
              >
                <SubIcon className="h-4 w-4 mr-2" />
                {subItem.label}
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}

function DropdownMenu({ item, isActive }: { item: NavItemWithSubmenu; isActive: boolean }) {
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const location = useLocation();

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const Icon = item.icon;

  return (
    <div className="relative" ref={dropdownRef}>
      <button
        onClick={() => setIsOpen(!isOpen)}
        className={`flex items-center px-4 py-2 rounded-md text-sm font-medium transition-colors ${
          isActive
            ? 'bg-blue-100 text-blue-700'
            : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
        }`}
      >
        <Icon className="h-4 w-4 mr-2" />
        {item.label}
        <ChevronDown className={`h-4 w-4 ml-1 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
      </button>
      {isOpen && (
        <div className="absolute top-full left-0 mt-1 w-48 bg-white rounded-md shadow-lg border border-gray-200 py-1 z-50">
          {item.submenu.map((subItem) => {
            if (isNavItemWithSubmenu(subItem)) {
              return <NestedSubmenu key={subItem.label} item={subItem} onClose={() => setIsOpen(false)} />;
            }
            const SubIcon = subItem.icon;
            const isSubActive = location.pathname === subItem.path;
            return (
              <Link
                key={subItem.path}
                to={subItem.path}
                onClick={() => setIsOpen(false)}
                className={`flex items-center px-4 py-2 text-sm transition-colors ${
                  isSubActive
                    ? 'bg-blue-50 text-blue-700'
                    : 'text-gray-700 hover:bg-gray-100'
                }`}
              >
                <SubIcon className="h-4 w-4 mr-2" />
                {subItem.label}
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function Layout({ children }: LayoutProps) {
  const location = useLocation();

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-16">
            <div className="flex items-center gap-4">
              <div className="flex items-center">
                <Database className="h-8 w-8 text-blue-600" />
                <h1 className="ml-3 text-xl font-bold text-gray-900">SQL RAG</h1>
              </div>
              <OperaVersionBadge />
              <CompanySelector />
            </div>
            <nav className="flex space-x-1">
              {navItems.map((item) => {
                if (isNavItemWithSubmenu(item)) {
                  const isActive = isItemActive(item, location.pathname);
                  return <DropdownMenu key={item.label} item={item} isActive={isActive} />;
                }

                const Icon = item.icon;
                const isActive = item.path === '/'
                  ? location.pathname === '/' || location.pathname.startsWith('/archive')
                  : location.pathname === item.path || location.pathname.startsWith(item.path);
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
