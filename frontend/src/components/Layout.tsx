import { useState, useRef, useEffect, type ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import {
  Database, Landmark, Settings, Lock, ChevronDown, ChevronRight,
  CreditCard, BookOpen, Users, Building2, Scale, Wrench, Truck,
  FileText, MessageSquare, Shield, LayoutDashboard, Receipt,
  Briefcase, FolderKanban, Package, ShoppingCart, ClipboardList,
  Cog, Activity, Boxes, LogOut, KeyRound, Send, RotateCcw, Monitor, CalendarDays
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

// --- Financials sub-groups ---

const cashbookUtilitiesSubmenu: NavItem[] = [
  { path: '/cashbook/options', label: 'Bank Rec Settings', icon: Settings },
  { path: '/cashbook/gocardless-settings', label: 'GoCardless Settings', icon: CreditCard },
  { path: '/cashbook/routines-cleardown', label: 'Routines Cleardown', icon: RotateCcw },
];

const cashbookSubmenu: (NavItem | NavItemWithSubmenu)[] = [
  { path: '/cashbook/bank-hub', label: 'Bank Statements', icon: Landmark },
  { path: '/cashbook/gocardless', label: 'GoCardless Import', icon: CreditCard },
  { path: '/cashbook/gocardless-requests', label: 'GoCardless Requests', icon: Send },
  { label: 'Utilities', icon: Wrench, submenu: cashbookUtilitiesSubmenu },
];

const payrollSubmenu: NavItem[] = [
  { path: '/payroll/pension-export', label: 'Pension Export', icon: FileText },
  { path: '/payroll/settings', label: 'Parameters', icon: Settings },
];

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

const operaModulesSubmenu: NavItem[] = [
  { path: '/stock', label: 'Stock', icon: Package },
  { path: '/sop', label: 'Sales Orders', icon: ShoppingCart },
  { path: '/pop', label: 'Purchase Orders', icon: ClipboardList },
  { path: '/bom', label: 'Works Orders', icon: Cog },
];

// --- Administration sub-groups ---

const balanceCheckSubmenu: NavItem[] = [
  { path: '/reconcile/summary', label: 'Summary', icon: Scale },
  { path: '/reconcile/trial-balance', label: 'Trial Balance', icon: Database },
  { path: '/reconcile/debtors', label: 'Debtors', icon: Users },
  { path: '/reconcile/creditors', label: 'Creditors', icon: Building2 },
  { path: '/reconcile/cashbook', label: 'Cashbook', icon: BookOpen },
  { path: '/reconcile/vat', label: 'VAT', icon: Receipt },
];

const utilitiesSubmenu: (NavItem | NavItemWithSubmenu)[] = [
  { label: 'Balance Check', icon: Scale, submenu: balanceCheckSubmenu },
  { path: '/utilities/user-activity', label: 'User Activity', icon: Activity },
];

const getAdministrationSubmenu = (isAdmin: boolean): (NavItem | NavItemWithSubmenu)[] => {
  const menu: (NavItem | NavItemWithSubmenu)[] = [
    { path: '/admin/company', label: 'Date & Company', icon: CalendarDays },
    { label: 'Utilities', icon: Wrench, submenu: utilitiesSubmenu },
    { path: '/admin/projects', label: 'Projects', icon: FolderKanban },
    { path: '/admin/lock-monitor', label: 'Lock Monitor', icon: Lock },
    { path: '/admin/installations', label: 'Installations', icon: Monitor },
    { path: '/settings', label: 'Settings', icon: Settings },
  ];

  if (isAdmin) {
    menu.push({ path: '/admin/users', label: 'Users', icon: Users });
    menu.push({ path: '/admin/licenses', label: 'Licenses', icon: KeyRound });
  }

  // Switch User is handled as a special action item — see SWITCH_USER_PATH
  menu.push({ path: '/action/switch-user', label: 'Switch User', icon: RotateCcw });

  return menu;
};

// Sentinel path used to detect "Switch User" clicks in the menu
const SWITCH_USER_PATH = '/action/switch-user';

// ============ COMPONENTS ============

function NestedSubmenu({ item, onClose, onAction }: { item: NavItemWithSubmenu; onClose: () => void; onAction?: (path: string) => boolean }) {
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
              return <NestedSubmenu key={subItem.label} item={subItem} onClose={onClose} onAction={onAction} />;
            }
            const SubIcon = subItem.icon;
            const isSubActive = location.pathname === subItem.path;
            // Check if this is an action item (e.g. Switch User)
            if (onAction && subItem.path.startsWith('/action/')) {
              return (
                <button
                  key={subItem.path}
                  onClick={() => { onClose(); onAction(subItem.path); }}
                  className="w-full flex items-center gap-2.5 px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 transition-colors"
                >
                  <SubIcon className="h-4 w-4 text-gray-400" />
                  <span>{subItem.label}</span>
                </button>
              );
            }
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

function DropdownMenu({ item, isActive, onOpenChange, onAction }: { item: NavItemWithSubmenu; isActive: boolean; onOpenChange?: (open: boolean) => void; onAction?: (path: string) => boolean }) {
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
        className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
          isActive
            ? 'bg-blue-50 text-blue-700'
            : 'text-gray-500 hover:bg-gray-50 hover:text-gray-900'
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
                  onAction={onAction}
                />
              );
            }
            const SubIcon = subItem.icon;
            const isSubActive = location.pathname === subItem.path;
            // Action items (e.g. Switch User)
            if (onAction && subItem.path.startsWith('/action/')) {
              return (
                <button
                  key={subItem.path}
                  onClick={() => { setIsOpen(false); onAction(subItem.path); }}
                  className="w-full flex items-center gap-2.5 px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 transition-colors"
                >
                  <SubIcon className="h-4 w-4 text-gray-400" />
                  <span>{subItem.label}</span>
                </button>
              );
            }
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
  const [showLogonConfirm, setShowLogonConfirm] = useState(false);
  const logonRef = useRef<HTMLDivElement>(null);

  // Close logon dropdown on outside click
  useEffect(() => {
    if (!showLogonConfirm) return;
    const handler = (e: MouseEvent) => {
      if (logonRef.current && !logonRef.current.contains(e.target as Node)) {
        setShowLogonConfirm(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showLogonConfirm]);
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

  // Get active system name
  const { data: activeSystemData } = useQuery({
    queryKey: ['activeSystem'],
    queryFn: async () => {
      const response = await apiClient.getActiveSystem();
      return response.data;
    },
  });
  const activeSystem = activeSystemData?.system;

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

  // Build nav items — two top-level groups mirroring Opera
  const filteredNavItems: NavEntry[] = [];

  // --- Financials (left) ---
  const financialsSubmenu: (NavItem | NavItemWithSubmenu)[] = [];
  if (hasPermission('cashbook')) {
    financialsSubmenu.push({ label: 'Cashbook', icon: BookOpen, submenu: cashbookSubmenu });
  }
  if (hasPermission('payroll')) {
    financialsSubmenu.push({ label: 'Payroll', icon: Briefcase, submenu: payrollSubmenu });
  }
  if (hasPermission('ap_automation')) {
    financialsSubmenu.push({ label: 'Suppliers', icon: Truck, submenu: suppliersSubmenu });
  }
  if (hasPermission('development')) {
    financialsSubmenu.push({ label: 'Opera Modules', icon: Boxes, submenu: operaModulesSubmenu });
  }
  if (financialsSubmenu.length > 0) {
    filteredNavItems.push({ label: 'Financials', icon: Landmark, submenu: financialsSubmenu });
  }

  // --- Administration (right) ---
  if (hasPermission('administration')) {
    filteredNavItems.push({
      label: 'Administration',
      icon: Settings,
      submenu: getAdministrationSubmenu(user?.is_admin || false),
    });
  }

  // Handle action items from menus (e.g. Switch User)
  const handleMenuAction = (path: string): boolean => {
    if (path === SWITCH_USER_PATH) {
      setShowLogonConfirm(true);
      return true;
    }
    return false;
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-40">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center h-14">
            {/* Logo & Brand */}
            <div className="flex items-center gap-4">
              <Link to="/" className="flex items-center gap-2.5 hover:opacity-80 transition-opacity">
                <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-blue-600 rounded-lg flex items-center justify-center">
                  <Database className="h-4.5 w-4.5 text-white" />
                </div>
                <span className="text-lg font-bold text-gray-900 tracking-tight">SQL RAG</span>
              </Link>
              <OperaVersionBadge />
            </div>

            {/* Navigation */}
            <nav className="flex items-center gap-0.5">
              {filteredNavItems.map((item) => {
                if (isNavItemWithSubmenu(item)) {
                  const isActive = isItemActive(item, location.pathname);
                  return (
                    <DropdownMenu
                      key={item.label}
                      item={item}
                      isActive={isActive}
                      onOpenChange={(open) => handleMenuOpenChange(item.label, open)}
                      onAction={handleMenuAction}
                    />
                  );
                }

                const Icon = item.icon;
                const isActive = location.pathname === item.path || location.pathname.startsWith(item.path + '/');
                return (
                  <Link
                    key={item.path}
                    to={item.path}
                    className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                      isActive
                        ? 'bg-blue-50 text-blue-700'
                        : 'text-gray-500 hover:bg-gray-50 hover:text-gray-900'
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
              <div className="flex items-center gap-3 pl-4 border-l border-gray-100">
                <div className="text-right">
                  <div className="text-sm font-medium text-gray-700">{user.display_name}</div>
                  {user.is_admin && (
                    <div className="text-xs text-blue-600 font-medium">Admin</div>
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
                    className="p-1.5 rounded-lg text-gray-400 hover:text-red-600 hover:bg-red-50 transition-colors"
                  >
                    <LogOut className="h-4 w-4" />
                  </button>
                )}
              </div>
            )}
          </div>
        </div>
      </header>

      {/* Switch User confirmation overlay */}
      {showLogonConfirm && user && (
        <div className="fixed inset-0 z-50 flex items-start justify-center pt-20">
          <div className="fixed inset-0 bg-black/20" onClick={() => setShowLogonConfirm(false)} />
          <div ref={logonRef} className="relative w-80 bg-white rounded-xl shadow-xl border border-gray-200 p-5 z-10">
            <p className="text-sm text-gray-700 mb-1">
              <span className="font-medium">Sign out</span> of <span className="font-semibold text-gray-900">{user.display_name || user.username}</span>?
            </p>
            <p className="text-xs text-gray-500 mb-4">You will be returned to the sign-in screen to log in as a different user.</p>
            <div className="flex items-center gap-2">
              <button
                onClick={() => { setShowLogonConfirm(false); logout(); }}
                className="flex-1 px-3 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 transition-colors"
              >
                Sign Out
              </button>
              <button
                onClick={() => setShowLogonConfirm(false)}
                className="flex-1 px-3 py-2 text-sm font-medium text-gray-600 bg-gray-100 rounded-lg hover:bg-gray-200 transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 pb-14">
        {showContent ? children : (
          <div className="flex items-center justify-center min-h-[60vh]">
            <p className="text-sm text-gray-400">Select an option from the menu above</p>
          </div>
        )}
      </main>

      {/* Status Bar */}
      <footer className="fixed bottom-0 left-0 right-0 bg-gray-900 text-white px-4 py-1.5 z-30">
        <div className="max-w-7xl mx-auto flex items-center justify-between text-xs">
          <div className="flex items-center gap-4">
            {activeSystem && (
              <div className="flex items-center gap-1.5">
                <Monitor className="h-3.5 w-3.5 text-gray-500" />
                <span className="text-gray-400">{activeSystem.name}</span>
              </div>
            )}
            {license && (
              <div className="flex items-center gap-1.5">
                <KeyRound className="h-3.5 w-3.5 text-gray-500" />
                <span className="text-gray-400">{license.client_name}</span>
                <span className="text-gray-600">Opera {license.opera_version}</span>
              </div>
            )}
            <div className="flex items-center gap-1.5">
              <Building2 className="h-3.5 w-3.5 text-gray-500" />
              <span className="text-gray-400">{typeof currentCompany === 'object' ? currentCompany?.name : currentCompany || 'Not selected'}</span>
            </div>
          </div>
          <div className="text-gray-600">
            ESC to go back
          </div>
        </div>
      </footer>
    </div>
  );
}
