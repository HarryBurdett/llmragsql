import { useState, useRef, useEffect, type ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Database, Landmark, Settings, Archive, Lock, ChevronDown, CreditCard, BookOpen } from 'lucide-react';
import { CompanySelector } from './CompanySelector';

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
  submenu: NavItem[];
}

const cashbookSubmenu: NavItem[] = [
  { path: '/cashbook/bank-rec', label: 'Opera Bank Rec', icon: Landmark },
  { path: '/cashbook/gocardless', label: 'GoCardless Import', icon: CreditCard },
];

const navItems: (NavItem | NavItemWithSubmenu)[] = [
  { label: 'Cashbook', icon: BookOpen, submenu: cashbookSubmenu },
  { path: '/', label: 'Archive', icon: Archive },
  { path: '/lock-monitor', label: 'Lock Monitor', icon: Lock },
  { path: '/settings', label: 'Settings', icon: Settings },
];

function isNavItemWithSubmenu(item: NavItem | NavItemWithSubmenu): item is NavItemWithSubmenu {
  return 'submenu' in item;
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
            <div className="flex items-center gap-6">
              <div className="flex items-center">
                <Database className="h-8 w-8 text-blue-600" />
                <h1 className="ml-3 text-xl font-bold text-gray-900">SQL RAG</h1>
              </div>
              <CompanySelector />
            </div>
            <nav className="flex space-x-1">
              {navItems.map((item, index) => {
                if (isNavItemWithSubmenu(item)) {
                  const isActive = item.submenu.some(
                    sub => location.pathname === sub.path || location.pathname.startsWith(sub.path)
                  );
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
