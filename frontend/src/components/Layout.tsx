import { useState, useRef, useEffect, type ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import {
  Database, Landmark, Settings, Lock, ChevronDown, ChevronRight,
  CreditCard, BookOpen, Users, Building2, Scale, Wrench, Truck,
  FileText, MessageSquare, Shield, LayoutDashboard, Receipt,
  Briefcase, FolderKanban, Package, ShoppingCart, ClipboardList,
  Cog, Activity, Boxes, LogOut, KeyRound, Send
} from 'lucide-react';
import { OperaVersionBadge } from './OperaVersionBadge';
import { useAuth } from '../context/AuthContext';
import { useUnsavedChanges } from '../context/UnsavedChangesContext';
import apiClient from '../api/client';

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
  module?: string;
}

type NavEntry = NavItem | NavItemWithSubmenu;

function isNavItemWithSubmenu(item: NavEntry): item is NavItemWithSubmenu {
  return 'submenu' in item;
}

function isItemActive(item: NavEntry, pathname: string): boolean {
  if (isNavItemWithSubmenu(item)) {
    return item.submenu.some(sub => isItemActive(sub, pathname));
  }
  return pathname === item.path || pathname.startsWith(item.path + '/');
}

// ============ MENU DEFINITIONS ============

const cashbookSubmenu: NavItem[] = [
  { path: '/cashbook/bank-rec', label: 'Bank Reconciliation', icon: Landmark },
  { path: '/cashbook/gocardless', label: 'GoCardless Import', icon: CreditCard },
  { path: '/cashbook/gocardless-requests', label: 'GoCardless Requests', icon: Send },
];

const payrollSubmenu: NavItem[] = [
  { path: '/payroll/pension-export', label: 'Pension Export', icon: FileText },
  { path: '/payroll/settings', label: 'Parameters', icon: Settings },
];

// Suppliers (AP Automation) - Flattened structure
const suppliersSubmenu: NavItem[] = [
  { path: '/supplier/dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { path: '/supplier/account', label: 'Account Lookup', icon: Receipt },
  { path: '/supplier/statements/queue', label: 'Statement Queue', icon: FileText },
  { path: '/supplier/statements/reconciliations', label: 'Reconciliations', icon: Scale },
  { path: '/supplier/queries/open', label: 'Open Queries', icon: MessageSquare },
  { path: '/supplier/directory', label: 'Directory', icon: Building2 },
  { path: '/supplier/communications', label: 'Communications', icon: MessageSquare },
  { path: '/supplier/security/alerts', label: 'Security Alerts', icon: Shield },
  { path: '/supplier/settings', label: 'Settings', icon: Settings },
];

// Balance Check submenu
const balanceCheckSubmenu: NavItem[] = [
  { path: '/reconcile/summary', label: 'Summary', icon: Scale },
  { path: '/reconcile/trial-balance', label: 'Trial Balance', icon: Database },
  { path: '/reconcile/debtors', label: 'Debtors', icon: Users },
  { path: '/reconcile/creditors', label: 'Creditors', icon: Building2 },
  { path: '/reconcile/cashbook', label: 'Cashbook', icon: BookOpen },
  { path: '/reconcile/vat', label: 'VAT', icon: Receipt },
];

// Utilities - simplified
const utilitiesSubmenu: (NavItem | NavItemWithSubmenu)[] = [
  { label: 'Balance Check', icon: Scale, submenu: balanceCheckSubmenu },
  { path: '/utilities/user-activity', label: 'User Activity', icon: Activity },
];

// Opera Modules (was Development > Opera SE)
const operaModulesSubmenu: NavItem[] = [
  { path: '/stock', label: 'Stock', icon: Package },
  { path: '/sop', label: 'Sales Orders', icon: ShoppingCart },
  { path: '/pop', label: 'Purchase Orders', icon: ClipboardList },
  { path: '/bom', label: 'Works Orders', icon: Cog },
];

// Administration submenu
const getAdministrationSubmenu = (isAdmin: boolean): NavItem[] => {
  const baseMenu: NavItem[] = [
    { path: '/admin/company', label: 'Company', icon: Building2 },
    { path: '/admin/projects', label: 'Projects', icon: FolderKanban },
    { path: '/admin/lock-monitor', label: 'Lock Monitor', icon: Lock },
    { path: '/settings', label: 'Settings', icon: Settings },
  ];

  if (isAdmin) {
    baseMenu.splice(2, 0, { path: '/admin/users', label: 'Users', icon: Users });
    baseMenu.splice(3, 0, { path: '/admin/licenses', label: 'Licenses', icon: KeyRound });
  }

  return baseMenu;
};

// ============ COMPONENTS ============

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
        onClick={() => setIsOpen(!isOpen)}
        className={`w-full flex items-center justify-between px-4 py-2.5 text-sm transition-colors ${
          isActive
            ? 'bg-blue-50 text-blue-700'
            : 'text-gray-700 hover:bg-gray-50'
        }`}
      >
        <div className="flex items-center gap-2.5">
          <Icon className="h-4 w-4 text-gray-400" />
          <span>{item.label}</span>
        </div>
        <ChevronRight className={`h-4 w-4 text-gray-400 transition-transform ${isOpen ? 'rotate-90' : ''}`} />
      </button>
      {isOpen && (
        <div className="absolute left-full top-0 ml-1 w-52 bg-white rounded-lg shadow-lg border border-gray-200 py-1.5 z-50">
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
                className={`flex items-center gap-2.5 px-4 py-2.5 text-sm transition-colors ${
                  isSubActive
                    ? 'bg-blue-50 text-blue-700'
                    : 'text-gray-700 hover:bg-gray-50'
                }`}
              >
                <SubIcon className="h-4 w-4 text-gray-400" />
                <span>{subItem.label}</span>
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}

function DropdownMenu({ item, isActive, onOpenChange }: { item: NavItemWithSubmenu; isActive: boolean; onOpenChange?: (open: boolean) => void }) {
  const [isOpen, setIsOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const location = useLocation();

  // Notify parent when open state changes
  useEffect(() => {
    if (onOpenChange) onOpenChange(isOpen);
  }, [isOpen, onOpenChange]);

  // Click outside handler
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Escape key handler - only active when menu is open
  useEffect(() => {
    if (!isOpen) return;

    function handleEscape(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        event.preventDefault();
        event.stopPropagation();
        setIsOpen(false);
        // Blur the button to prevent focus issues
        buttonRef.current?.blur();
      }
    }
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [isOpen]);

  // Handle button click - explicitly toggle the menu
  const handleButtonClick = (event: React.MouseEvent) => {
    event.preventDefault();
    event.stopPropagation();
    setIsOpen(prev => !prev);
  };

  const Icon = item.icon;

  return (
    <div className="relative" ref={dropdownRef}>
      <button
        ref={buttonRef}
        onClick={handleButtonClick}
        className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all ${
          isActive
            ? 'bg-blue-100 text-blue-700'
            : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
        }`}
      >
        <Icon className="h-4 w-4" />
        <span>{item.label}</span>
        <ChevronDown className={`h-3.5 w-3.5 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
      </button>
      {isOpen && (
        <div className="absolute top-full right-0 mt-1.5 w-52 bg-white rounded-lg shadow-lg border border-gray-200 py-1.5 z-50">
          {item.submenu.map((subItem) => {
            if (isNavItemWithSubmenu(subItem)) {
              return (
                <NestedSubmenu
                  key={subItem.label}
                  item={subItem}
                  onClose={() => setIsOpen(false)}
                />
              );
            }
            const SubIcon = subItem.icon;
            const isSubActive = location.pathname === subItem.path;
            return (
              <Link
                key={subItem.path}
                to={subItem.path}
                onClick={() => setIsOpen(false)}
                className={`flex items-center gap-2.5 px-4 py-2.5 text-sm transition-colors ${
                  isSubActive
                    ? 'bg-blue-50 text-blue-700'
                    : 'text-gray-700 hover:bg-gray-50'
                }`}
              >
                <SubIcon className="h-4 w-4 text-gray-400" />
                <span>{subItem.label}</span>
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}

// ============ MAIN LAYOUT ============

export function Layout({ children }: LayoutProps) {
  const location = useLocation();
  const { user, logout, hasPermission, license } = useAuth();
  const { confirmNavigation, setHasUnsavedChanges } = useUnsavedChanges();
  const [anyMenuOpen, setAnyMenuOpen] = useState(false);
  const [showContent, setShowContent] = useState(true);
  const openMenusRef = useRef<Set<string>>(new Set());

  // Get current company
  const { data: companiesData } = useQuery({
    queryKey: ['companies'],
    queryFn: async () => {
      const response = await apiClient.getCompanies();
      return response.data;
    },
  });
  const currentCompany = companiesData?.current_company;

  // Track which menus are open
  const handleMenuOpenChange = (menuLabel: string, isOpen: boolean) => {
    if (isOpen) {
      openMenusRef.current.add(menuLabel);
    } else {
      openMenusRef.current.delete(menuLabel);
    }
    setAnyMenuOpen(openMenusRef.current.size > 0);
  };

  // Global Escape handler - navigate back or clear content when no menus/modals are open
  useEffect(() => {
    async function handleGlobalEscape(event: KeyboardEvent) {
      if (event.key !== 'Escape') return;

      // Don't handle if a dropdown menu is open (let DropdownMenu handle it)
      if (anyMenuOpen) return;

      // Don't handle if a modal is open (check for modal overlay)
      const modalOverlay = document.querySelector('.fixed.inset-0.bg-black');
      if (modalOverlay) return;

      // Don't handle if already handled by something else
      if (event.defaultPrevented) return;

      // If on a sub-page, go back to home
      if (location.pathname !== '/' && showContent) {
        event.preventDefault();

        // Check for unsaved changes before navigating
        const canNavigate = await confirmNavigation();
        if (canNavigate) {
          window.history.back();
        }
      }
    }

    // Use bubble phase so modals/dropdowns can handle first
    document.addEventListener('keydown', handleGlobalEscape);
    return () => document.removeEventListener('keydown', handleGlobalEscape);
  }, [anyMenuOpen, showContent, location.pathname, confirmNavigation]);

  // Show content when navigating to a new page and clear unsaved changes flag
  useEffect(() => {
    setShowContent(true);
    // Clear unsaved changes when navigating to a new page
    setHasUnsavedChanges(false);
  }, [location.pathname, setHasUnsavedChanges]);

  // Build nav items based on permissions
  const filteredNavItems: NavEntry[] = [];

  // Cashbook
  if (hasPermission('cashbook')) {
    filteredNavItems.push({ label: 'Cashbook', icon: BookOpen, submenu: cashbookSubmenu });
  }

  // Payroll
  if (hasPermission('payroll')) {
    filteredNavItems.push({ label: 'Payroll', icon: Briefcase, submenu: payrollSubmenu });
  }

  // Suppliers (was AP Automation)
  if (hasPermission('ap_automation')) {
    filteredNavItems.push({ label: 'Suppliers', icon: Truck, submenu: suppliersSubmenu });
  }

  // Utilities
  if (hasPermission('utilities')) {
    filteredNavItems.push({ label: 'Utilities', icon: Wrench, submenu: utilitiesSubmenu });
  }

  // Opera Modules (was Development)
  if (hasPermission('development')) {
    filteredNavItems.push({ label: 'Opera', icon: Boxes, submenu: operaModulesSubmenu });
  }

  // Administration
  if (hasPermission('administration')) {
    filteredNavItems.push({
      label: 'Admin',
      icon: Settings,
      submenu: getAdministrationSubmenu(user?.is_admin || false),
    });
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200 sticky top-0 z-40">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-14">
            {/* Logo, Brand & Logout */}
            <div className="flex items-center gap-4">
              {/* Logon button on far left - allows switching users */}
              {user && (
                <button
                  onClick={() => {
                    if (window.confirm('Log out and switch user?')) {
                      logout();
                    }
                  }}
                  className="px-3 py-1.5 text-sm font-medium text-gray-600 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-colors border border-gray-300"
                >
                  Logon
                </button>
              )}
              <Link to="/" className="flex items-center gap-2 hover:opacity-80 transition-opacity">
                <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-blue-600 rounded-lg flex items-center justify-center">
                  <Database className="h-5 w-5 text-white" />
                </div>
                <span className="text-lg font-semibold text-gray-900">SQL RAG</span>
              </Link>
              <OperaVersionBadge />
            </div>

            {/* Navigation */}
            <nav className="flex items-center gap-1">
              {filteredNavItems.map((item) => {
                if (isNavItemWithSubmenu(item)) {
                  const isActive = isItemActive(item, location.pathname);
                  return (
                    <DropdownMenu
                      key={item.label}
                      item={item}
                      isActive={isActive}
                      onOpenChange={(open) => handleMenuOpenChange(item.label, open)}
                    />
                  );
                }

                const Icon = item.icon;
                const isActive = location.pathname === item.path || location.pathname.startsWith(item.path + '/');
                return (
                  <Link
                    key={item.path}
                    to={item.path}
                    className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all ${
                      isActive
                        ? 'bg-blue-100 text-blue-700'
                        : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
                    }`}
                  >
                    <Icon className="h-4 w-4" />
                    <span>{item.label}</span>
                  </Link>
                );
              })}
            </nav>

            {/* User Info & Exit */}
            {user && (
              <div className="flex items-center gap-3 pl-4 border-l border-gray-200">
                <div className="text-right">
                  <div className="text-sm font-medium text-gray-900">{user.display_name}</div>
                  {user.is_admin && (
                    <div className="text-xs text-purple-600 font-medium">Admin</div>
                  )}
                </div>
                {/* Exit button only visible when no menus are open */}
                {!anyMenuOpen && (
                  <button
                    onClick={() => {
                      if (window.confirm('Exit the system?')) {
                        logout();
                      }
                    }}
                    title="Exit"
                    className="p-2 rounded-lg text-gray-400 hover:text-red-600 hover:bg-red-50 transition-colors"
                  >
                    <LogOut className="h-5 w-5" />
                  </button>
                )}
              </div>
            )}
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 pb-16">
        {showContent ? children : (
          <div className="flex items-center justify-center min-h-[60vh]">
            <p className="text-sm text-gray-400">Select an option from the menu above</p>
          </div>
        )}
      </main>

      {/* Status Bar */}
      <footer className="fixed bottom-0 left-0 right-0 bg-gray-800 text-white px-4 py-2 z-30">
        <div className="max-w-7xl mx-auto flex items-center justify-between text-sm">
          <div className="flex items-center gap-4">
            {license && (
              <div className="flex items-center gap-2">
                <KeyRound className="h-4 w-4 text-purple-400" />
                <span className="text-gray-300">Client:</span>
                <span className="font-medium">{license.client_name}</span>
                <span className="text-xs text-gray-500">(Opera {license.opera_version})</span>
              </div>
            )}
            <div className="flex items-center gap-2">
              <Building2 className="h-4 w-4 text-blue-400" />
              <span className="text-gray-300">Company:</span>
              <span className="font-medium">{typeof currentCompany === 'object' ? currentCompany?.name : currentCompany || 'Not selected'}</span>
            </div>
          </div>
          <div className="text-gray-400 text-xs">
            Press ESC to return to menu
          </div>
        </div>
      </footer>
    </div>
  );
}
