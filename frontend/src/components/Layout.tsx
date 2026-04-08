import { useState, useRef, useEffect, useCallback, type ReactNode } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import {
  Database, Landmark, Settings, Lock, ChevronDown,
  CreditCard, BookOpen, Users, Building2, Scale, Truck,
  FileText, MessageSquare, Shield, LayoutDashboard, Receipt,
  Briefcase, FolderKanban, Package, ShoppingCart, ClipboardList,
  Cog, Activity, LogOut, KeyRound, Send, RotateCcw, Monitor, CalendarDays,
  Home, User
} from 'lucide-react';
import { OperaVersionBadge } from './OperaVersionBadge';
import { Opera3AgentStatus } from './Opera3AgentStatus';
import { VoiceButton } from './VoiceButton';
import { VoiceIndicator } from './VoiceIndicator';
import { useAuth } from '../context/AuthContext';
import { useUnsavedChanges } from '../context/UnsavedChangesContext';
import apiClient from '../api/client';

interface LayoutProps {
  children: ReactNode;
}

interface MenuItem {
  path: string;
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  description?: string;
  color?: string;
}

interface MenuSection {
  heading?: string;
  items: MenuItem[];
}

interface TopLevelMenu {
  label: string;
  icon: React.ComponentType<{ className?: string }>;
  sections: MenuSection[];
}

// Color map for graphical tiles (matches LauncherTile style)
const tileColors: Record<string, { bg: string; iconBg: string; icon: string; border: string; hoverBorder: string }> = {
  blue:    { bg: 'bg-blue-50',    iconBg: 'bg-blue-100',    icon: 'text-blue-600',    border: 'border-blue-100',    hoverBorder: 'hover:border-blue-300' },
  emerald: { bg: 'bg-emerald-50', iconBg: 'bg-emerald-100', icon: 'text-emerald-600', border: 'border-emerald-100', hoverBorder: 'hover:border-emerald-300' },
  purple:  { bg: 'bg-purple-50',  iconBg: 'bg-purple-100',  icon: 'text-purple-600',  border: 'border-purple-100',  hoverBorder: 'hover:border-purple-300' },
  amber:   { bg: 'bg-amber-50',   iconBg: 'bg-amber-100',   icon: 'text-amber-600',   border: 'border-amber-100',   hoverBorder: 'hover:border-amber-300' },
  rose:    { bg: 'bg-rose-50',    iconBg: 'bg-rose-100',    icon: 'text-rose-600',    border: 'border-rose-100',    hoverBorder: 'hover:border-rose-300' },
  indigo:  { bg: 'bg-indigo-50',  iconBg: 'bg-indigo-100',  icon: 'text-indigo-600',  border: 'border-indigo-100',  hoverBorder: 'hover:border-indigo-300' },
  orange:  { bg: 'bg-orange-50',  iconBg: 'bg-orange-100',  icon: 'text-orange-600',  border: 'border-orange-100',  hoverBorder: 'hover:border-orange-300' },
  slate:   { bg: 'bg-slate-50',   iconBg: 'bg-slate-100',   icon: 'text-slate-600',   border: 'border-slate-100',   hoverBorder: 'hover:border-slate-300' },
  cyan:    { bg: 'bg-cyan-50',    iconBg: 'bg-cyan-100',    icon: 'text-cyan-600',    border: 'border-cyan-100',    hoverBorder: 'hover:border-cyan-300' },
  teal:    { bg: 'bg-teal-50',    iconBg: 'bg-teal-100',    icon: 'text-teal-600',    border: 'border-teal-100',    hoverBorder: 'hover:border-teal-300' },
};

// ============ FLAT MENU DEFINITIONS ============
// All items are max 1 level deep — no nested fly-outs

// ============ WORKFLOW MENUS (daily use) ============

const cashbookMenu: TopLevelMenu = {
  label: 'Cashbook',
  icon: BookOpen,
  sections: [
    {
      items: [
        { path: '/cashbook/bank-hub', label: 'Bank Statements', icon: Landmark, description: 'Import & reconcile bank statements', color: 'blue' },
      ],
    },
  ],
};

const gocardlessMenu: TopLevelMenu = {
  label: 'GoCardless',
  icon: CreditCard,
  sections: [
    {
      items: [
        { path: '/cashbook/gocardless', label: 'Import', icon: CreditCard, description: 'Import direct debit payments', color: 'emerald' },
        { path: '/cashbook/gocardless-requests', label: 'Payment Requests', icon: Send, description: 'Create & manage DD requests', color: 'purple' },
      ],
    },
  ],
};

const suppliersMenu: TopLevelMenu = {
  label: 'Suppliers',
  icon: Truck,
  sections: [
    {
      items: [
        { path: '/supplier/dashboard', label: 'Dashboard', icon: LayoutDashboard, description: 'Overview and alerts', color: 'amber' },
        { path: '/supplier/statements/queue', label: 'Statements', icon: FileText, description: 'Review supplier statements', color: 'blue' },
        { path: '/supplier/directory', label: 'Supplier Directory', icon: Building2, description: 'Supplier list and settings', color: 'purple' },
        { path: '/supplier/aged-creditors', label: 'Aged Creditors', icon: Receipt, description: 'Outstanding balances by age', color: 'teal' },
      ],
    },
  ],
};

const utilitiesMenu: TopLevelMenu = {
  label: 'Utilities',
  icon: Scale,
  sections: [
    {
      heading: 'Balance Check',
      items: [
        { path: '/reconcile/summary', label: 'Summary', icon: Scale, description: 'Overall balance status', color: 'teal' },
        { path: '/reconcile/trial-balance', label: 'Trial Balance', icon: Database, description: 'Nominal trial balance', color: 'blue' },
        { path: '/reconcile/debtors', label: 'Debtors', icon: Users, description: 'Sales ledger vs control', color: 'emerald' },
        { path: '/reconcile/creditors', label: 'Creditors', icon: Building2, description: 'Purchase ledger vs control', color: 'amber' },
        { path: '/reconcile/cashbook', label: 'Cashbook', icon: BookOpen, description: 'Bank vs nominal check', color: 'purple' },
        { path: '/reconcile/vat', label: 'VAT', icon: Receipt, description: 'VAT return reconciliation', color: 'rose' },
      ],
    },
    {
      heading: 'Developer Tools',
      items: [
        { path: '/utilities/transaction-snapshot', label: 'Transaction Snapshot', icon: Database, description: 'Capture Opera posting patterns', color: 'indigo' },
        { path: '/utilities/transaction-monitor', label: 'Transaction Monitor', icon: Activity, description: 'Monitor live Opera systems', color: 'green' },
      ],
    },
  ],
};

const getAdminMenu = (isAdmin: boolean): TopLevelMenu => ({
  label: 'Admin',
  icon: Settings,
  sections: [
    {
      items: [
        { path: '/admin/company', label: 'Date & Company', icon: CalendarDays, description: 'Switch company & period', color: 'blue' },
        { path: '/admin/installations', label: 'Installations', icon: Monitor, description: 'Connected systems', color: 'indigo' },
        ...(isAdmin ? [
          { path: '/admin/users', label: 'Users', icon: Users, description: 'Manage user accounts', color: 'amber' },
          { path: '/admin/licenses', label: 'Licenses', icon: KeyRound, description: 'License management', color: 'orange' },
        ] : []),
      ],
    },
    {
      heading: 'Module Setup',
      items: [
        { path: '/cashbook/options', label: 'Bank Rec Settings', icon: Settings, description: 'Configure bank reconciliation', color: 'slate' },
        { path: '/cashbook/routines-cleardown', label: 'Routines Cleardown', icon: RotateCcw, description: 'Reset routine flags', color: 'slate' },
        { path: '/cashbook/gocardless-settings', label: 'GoCardless Settings', icon: CreditCard, description: 'API keys & connection', color: 'slate' },
        { path: '/supplier/settings', label: 'Supplier Settings', icon: Truck, description: 'Automation parameters', color: 'slate' },
        { path: '/settings', label: 'Application Settings', icon: Settings, description: 'Global configuration', color: 'slate' },
        { path: '/admin/lock-monitor', label: 'Lock Monitor', icon: Lock, description: 'Active database locks', color: 'rose' },
      ],
    },
    {
      heading: 'Apps Archive',
      items: [
        { path: '/payroll/pension-export', label: 'Pension Export', icon: FileText, description: 'Generate pension submissions', color: 'indigo' },
        { path: '/payroll/settings', label: 'Payroll Parameters', icon: Briefcase, description: 'Payroll configuration', color: 'slate' },
        { path: '/stock', label: 'Stock', icon: Package, description: 'Stock control & movements', color: 'emerald' },
        { path: '/sop/batch-processing', label: 'SOP Batch Processing', icon: ShoppingCart, description: 'Progress documents through stages', color: 'blue' },
        { path: '/pop', label: 'Purchase Orders', icon: ClipboardList, description: 'Purchase order processing', color: 'amber' },
        { path: '/bom', label: 'Works Orders', icon: Cog, description: 'Bill of materials & works', color: 'purple' },
        { path: '/admin/system-dashboard', label: 'System Dashboard', icon: Activity, description: 'System health & status', color: 'emerald' },
        { path: '/admin/projects', label: 'Ideas Archive', icon: FolderKanban, description: 'Project ideas & proposals', color: 'purple' },
      ],
    },
  ],
});

function isMenuActive(menu: TopLevelMenu, pathname: string): boolean {
  return menu.sections.some(section =>
    section.items.some(item => pathname === item.path || pathname.startsWith(item.path + '/'))
  );
}

// ============ MEGA DROPDOWN ============

function MegaDropdown({
  menu,
  isOpen,
  onToggle,
  onClose,
  pathname,
  graphical,
}: {
  menu: TopLevelMenu;
  isOpen: boolean;
  onToggle: () => void;
  onClose: () => void;
  pathname: string;
  graphical: boolean;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const active = isMenuActive(menu, pathname);
  const Icon = menu.icon;

  // Close on outside click
  useEffect(() => {
    if (!isOpen) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [isOpen, onClose]);

  // Close on Escape
  useEffect(() => {
    if (!isOpen) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        e.stopPropagation();
        onClose();
        buttonRef.current?.blur();
      }
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [isOpen, onClose]);

  // Total items for layout decisions
  const totalItems = menu.sections.reduce((count, s) => count + s.items.length, 0);

  return (
    <div className="relative" ref={ref}>
      <button
        ref={buttonRef}
        onClick={(e) => { e.preventDefault(); e.stopPropagation(); onToggle(); }}
        className={`
          flex items-center gap-1.5 px-3 py-1.5 rounded-md text-[13px] font-medium transition-all duration-150
          ${active
            ? 'text-blue-700 bg-blue-50/80'
            : isOpen
              ? 'text-gray-900 bg-gray-100'
              : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
          }
        `}
      >
        <Icon className="h-4 w-4" />
        <span>{menu.label}</span>
        <ChevronDown className={`h-3 w-3 opacity-50 transition-transform duration-200 ${isOpen ? 'rotate-180' : ''}`} />
      </button>

      {/* Dropdown panel */}
      {isOpen && !graphical && (
        <ClassicDropdown menu={menu} pathname={pathname} onClose={onClose} totalItems={totalItems} />
      )}
      {isOpen && graphical && (
        <GraphicalDropdown menu={menu} pathname={pathname} onClose={onClose} totalItems={totalItems} />
      )}
    </div>
  );
}

// ============ CLASSIC (compact) DROPDOWN ============

function ClassicDropdown({ menu, pathname, onClose, totalItems }: { menu: TopLevelMenu; pathname: string; onClose: () => void; totalItems: number }) {
  const hasManyItems = totalItems > 8;
  const multiColumn = menu.sections.length > 1 && hasManyItems;

  return (
    <div
      className={`
        absolute top-full left-0 mt-1 bg-white rounded-xl shadow-xl border border-gray-200/80
        ring-1 ring-black/5 overflow-hidden z-50
        ${multiColumn ? 'min-w-[420px]' : 'min-w-[220px]'}
      `}
      style={{ animation: 'menuIn 150ms ease-out' }}
    >
      <div className={multiColumn ? 'grid grid-cols-2 gap-0' : ''}>
        {menu.sections.map((section, si) => (
          <div
            key={si}
            className={`
              py-2 px-1
              ${multiColumn && si > 0 ? 'border-l border-gray-100' : ''}
              ${!multiColumn && si > 0 ? 'border-t border-gray-100' : ''}
            `}
          >
            {section.heading && (
              <div className="px-3 pt-1 pb-2">
                <span className="text-[10px] font-semibold uppercase tracking-wider text-gray-400">
                  {section.heading}
                </span>
              </div>
            )}
            {section.items.map((item) => {
              const ItemIcon = item.icon;
              const itemActive = pathname === item.path || pathname.startsWith(item.path + '/');
              return (
                <Link
                  key={item.path}
                  to={item.path}
                  onClick={onClose}
                  className={`
                    flex items-center gap-2.5 mx-1 px-3 py-2 rounded-lg text-[13px] transition-all duration-100
                    ${itemActive
                      ? 'bg-blue-50 text-blue-700 font-medium'
                      : 'text-gray-700 hover:bg-gray-50 hover:text-gray-900'
                    }
                  `}
                >
                  <ItemIcon className={`h-4 w-4 flex-shrink-0 ${itemActive ? 'text-blue-500' : 'text-gray-400'}`} />
                  <span>{item.label}</span>
                </Link>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
}

// ============ GRAPHICAL (tile) DROPDOWN ============

function GraphicalDropdown({ menu, pathname, onClose, totalItems }: { menu: TopLevelMenu; pathname: string; onClose: () => void; totalItems: number }) {
  // Compact mode for large menus (Admin has 20+ items)
  const compact = totalItems > 12;
  const cols = compact ? 4 : totalItems > 6 ? 3 : totalItems > 3 ? 2 : 1;

  return (
    <div
      className="absolute top-full left-0 mt-1.5 bg-white rounded-2xl shadow-2xl border border-gray-200/60 ring-1 ring-black/5 overflow-hidden z-50 max-h-[80vh] overflow-y-auto"
      style={{
        animation: 'menuIn 180ms ease-out',
        minWidth: cols >= 4 ? '620px' : cols === 3 ? '580px' : cols === 2 ? '400px' : '220px',
      }}
    >
      {menu.sections.map((section, si) => (
        <div key={si} className={si > 0 ? 'border-t border-gray-100' : ''}>
          {section.heading && (
            <div className={compact ? 'px-4 pt-2 pb-0.5' : 'px-5 pt-3 pb-1'}>
              <span className="text-[10px] font-bold uppercase tracking-widest text-gray-400">
                {section.heading}
              </span>
            </div>
          )}
          <div
            className={compact ? 'p-2 gap-1.5' : 'p-3 gap-2'}
            style={{
              display: 'grid',
              gridTemplateColumns: `repeat(${Math.min(cols, section.items.length)}, 1fr)`,
            }}
          >
            {section.items.map((item) => {
              const ItemIcon = item.icon;
              const itemActive = pathname === item.path || pathname.startsWith(item.path + '/');
              const c = tileColors[item.color || 'blue'] || tileColors.blue;

              return (
                <Link
                  key={item.path}
                  to={item.path}
                  onClick={onClose}
                  className={`
                    group flex flex-col items-center text-center ${compact ? 'p-2 rounded-lg' : 'p-4 rounded-xl'} border transition-all duration-150
                    ${itemActive
                      ? `${c.bg} ${c.border} ring-2 ring-blue-200 shadow-sm`
                      : `bg-white border-gray-100 ${c.hoverBorder} hover:shadow-md hover:scale-[1.02]`
                    }
                  `}
                >
                  <div className={`${compact ? 'p-1.5 rounded-lg mb-1' : 'p-2.5 rounded-xl mb-2.5'} ${c.iconBg} transition-transform group-hover:scale-110`}>
                    <ItemIcon className={`${compact ? 'h-4 w-4' : 'h-5 w-5'} ${c.icon}`} />
                  </div>
                  <div className={`${compact ? 'text-[11px]' : 'text-[13px]'} font-semibold text-gray-800 leading-tight`}>{item.label}</div>
                  {!compact && item.description && (
                    <div className="text-[11px] text-gray-400 mt-1 leading-snug">{item.description}</div>
                  )}
                </Link>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}

// ============ MAIN LAYOUT ============

export function Layout({ children }: LayoutProps) {
  const location = useLocation();
  const { user, logout, hasPermission, license } = useAuth();
  const { confirmNavigation, setHasUnsavedChanges } = useUnsavedChanges();
  const [openMenu, setOpenMenu] = useState<string | null>(null);
  const [showContent, setShowContent] = useState(true);
  const [showLogonConfirm, setShowLogonConfirm] = useState(false);
  const [showUserMenu, setShowUserMenu] = useState(false);
  const logonRef = useRef<HTMLDivElement>(null);
  const userMenuRef = useRef<HTMLDivElement>(null);

  const graphical = user?.ui_mode === 'launcher';

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

  // Close user menu on outside click
  useEffect(() => {
    if (!showUserMenu) return;
    const handler = (e: MouseEvent) => {
      if (userMenuRef.current && !userMenuRef.current.contains(e.target as Node)) {
        setShowUserMenu(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showUserMenu]);

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

  const closeMenu = useCallback(() => setOpenMenu(null), []);

  const toggleMenu = useCallback((label: string) => {
    setOpenMenu(prev => prev === label ? null : label);
  }, []);

  // Global Escape handler
  useEffect(() => {
    async function handleGlobalEscape(event: KeyboardEvent) {
      if (event.key !== 'Escape') return;
      if (openMenu) return;

      const modalOverlay = document.querySelector('.fixed.inset-0.bg-black');
      if (modalOverlay) return;
      if (event.defaultPrevented) return;

      if (location.pathname !== '/' && showContent) {
        event.preventDefault();
        const canNavigate = await confirmNavigation();
        if (canNavigate) {
          window.history.back();
        }
      }
    }
    document.addEventListener('keydown', handleGlobalEscape);
    return () => document.removeEventListener('keydown', handleGlobalEscape);
  }, [openMenu, showContent, location.pathname, confirmNavigation]);

  // Show content when navigating
  useEffect(() => {
    setShowContent(true);
    setHasUnsavedChanges(false);
  }, [location.pathname, setHasUnsavedChanges]);

  // Build menus based on permissions
  const menus: TopLevelMenu[] = [];
  if (hasPermission('cashbook')) menus.push(cashbookMenu);
  if (hasPermission('cashbook')) menus.push(gocardlessMenu);
  if (hasPermission('ap_automation')) menus.push(suppliersMenu);
  menus.push(utilitiesMenu);
  if (hasPermission('administration')) menus.push(getAdminMenu(
    user?.is_admin || false,
  ));

  // User initials for avatar
  const userInitials = user?.display_name
    ? user.display_name.split(' ').map((n: string) => n[0]).join('').toUpperCase().slice(0, 2)
    : '?';

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Inject animation keyframes */}
      <style>{`
        @keyframes menuIn {
          from { opacity: 0; transform: translateY(-4px); }
          to { opacity: 1; transform: translateY(0); }
        }
      `}</style>

      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-40">
        {/* Accent line */}
        <div className="h-[2px]" style={{ background: 'linear-gradient(90deg, #3b82f6, #8b5cf6, #3b82f6)' }} />

        <div className="max-w-[1400px] mx-auto px-4 sm:px-6">
          <div className="flex items-center h-12 gap-1">
            {/* Logo */}
            <Link to="/" className="flex items-center gap-2 mr-4 hover:opacity-80 transition-opacity flex-shrink-0">
              <div
                className="w-7 h-7 rounded-lg flex items-center justify-center font-extrabold text-xs text-white shadow-sm"
                style={{ background: 'linear-gradient(135deg, #3b82f6, #8b5cf6)' }}
              >
                C
              </div>
              <span className="text-base font-bold text-gray-800 hidden sm:inline">
                Crakd<span className="text-blue-500">.ai</span>
              </span>
            </Link>

            {/* Divider */}
            <div className="h-5 w-px bg-gray-200 mx-1 flex-shrink-0" />

            {/* Home link */}
            <Link
              to="/"
              className={`
                flex items-center gap-1.5 px-3 py-1.5 rounded-md text-[13px] font-medium transition-all duration-150
                ${location.pathname === '/'
                  ? 'text-blue-700 bg-blue-50/80'
                  : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
                }
              `}
            >
              <Home className="h-4 w-4" />
              <span className="hidden md:inline">Home</span>
            </Link>

            {/* Navigation menus */}
            <nav className="flex items-center gap-0.5">
              {menus.map((menu) => (
                <MegaDropdown
                  key={menu.label}
                  menu={menu}
                  isOpen={openMenu === menu.label}
                  onToggle={() => toggleMenu(menu.label)}
                  onClose={closeMenu}
                  pathname={location.pathname}
                  graphical={graphical}
                />
              ))}
            </nav>

            {/* Right side spacer */}
            <div className="flex-1" />

            {/* Opera badges */}
            <div className="flex items-center gap-2 mr-2">
              <OperaVersionBadge />
              <Opera3AgentStatus />
            </div>

            {/* Voice Control */}
            <VoiceButton />

            {/* User area */}
            {user && (
              <div className="flex items-center gap-1 flex-shrink-0 ml-2">
                {/* User avatar + dropdown */}
                <div className="relative" ref={userMenuRef}>
                  <button
                    onClick={() => setShowUserMenu(!showUserMenu)}
                    className="flex items-center gap-2 pl-1 pr-2 py-1 rounded-lg hover:bg-gray-50 transition-colors"
                  >
                    <div
                      className="w-7 h-7 rounded-full flex items-center justify-center text-[11px] font-bold text-white shadow-sm"
                      style={{ background: 'linear-gradient(135deg, #3b82f6, #8b5cf6)' }}
                    >
                      {userInitials}
                    </div>
                    <div className="text-right hidden sm:block">
                      <div className="text-[13px] font-medium text-gray-700 leading-tight">{user.display_name}</div>
                      {user.is_admin && (
                        <div className="text-[10px] text-blue-600 font-semibold leading-tight">Admin</div>
                      )}
                    </div>
                    <ChevronDown className="h-3 w-3 text-gray-400" />
                  </button>

                  {/* User dropdown */}
                  {showUserMenu && (
                    <div
                      className="absolute right-0 top-full mt-1 w-56 bg-white rounded-xl shadow-xl border border-gray-200/80 ring-1 ring-black/5 overflow-hidden z-50"
                      style={{ animation: 'menuIn 150ms ease-out' }}
                    >
                      <div className="px-4 py-3 border-b border-gray-100 bg-gray-50/50">
                        <p className="text-sm font-medium text-gray-900">{user.display_name}</p>
                        <p className="text-xs text-gray-500 mt-0.5">{user.username}</p>
                      </div>
                      <div className="py-1.5">
                        <Link
                          to="/my-preferences"
                          onClick={() => setShowUserMenu(false)}
                          className="flex items-center gap-2.5 mx-1 px-3 py-2 rounded-lg text-[13px] text-gray-700 hover:bg-gray-50 transition-colors"
                        >
                          <User className="h-4 w-4 text-gray-400" />
                          <span>My Preferences</span>
                        </Link>
                        <button
                          onClick={() => { setShowUserMenu(false); setShowLogonConfirm(true); }}
                          className="w-full flex items-center gap-2.5 mx-1 px-3 py-2 rounded-lg text-[13px] text-gray-700 hover:bg-gray-50 transition-colors"
                          style={{ width: 'calc(100% - 8px)' }}
                        >
                          <Users className="h-4 w-4 text-gray-400" />
                          <span>Switch User</span>
                        </button>
                      </div>
                    </div>
                  )}
                </div>

                {/* Prominent Sign Out button — always visible */}
                <button
                  onClick={() => setShowLogonConfirm(true)}
                  title="Sign Out"
                  className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg text-[13px] font-medium text-gray-500 hover:text-red-600 hover:bg-red-50 transition-colors"
                >
                  <LogOut className="h-4 w-4" />
                  <span className="hidden lg:inline">Sign Out</span>
                </button>
              </div>
            )}
          </div>
        </div>
      </header>

      {/* Switch User confirmation overlay */}
      {showLogonConfirm && user && (
        <div className="fixed inset-0 z-50 flex items-start justify-center pt-20">
          <div className="fixed inset-0 bg-black/20 backdrop-blur-[1px]" onClick={() => setShowLogonConfirm(false)} />
          <div
            ref={logonRef}
            className="relative w-80 bg-white rounded-xl shadow-xl border border-gray-200 p-5 z-10"
            style={{ animation: 'menuIn 200ms ease-out' }}
          >
            <p className="text-sm text-gray-700 mb-1">
              <span className="font-medium">Sign out</span> of <span className="font-semibold text-gray-900">{user.display_name || user.username}</span>?
            </p>
            <p className="text-xs text-gray-500 mb-4">You will be returned to the sign-in screen.</p>
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
      <main className="max-w-[1400px] mx-auto px-4 sm:px-6 py-6 pb-14">
        {showContent ? children : (
          <div className="flex items-center justify-center min-h-[60vh]">
            <p className="text-sm text-gray-400">Select an option from the menu above</p>
          </div>
        )}
      </main>

      {/* Voice Indicator */}
      <VoiceIndicator />

      {/* Status Bar */}
      <footer className="fixed bottom-0 left-0 right-0 bg-gray-900 text-white px-4 py-1 z-30">
        <div className="max-w-[1400px] mx-auto flex items-center justify-between text-[11px]">
          <div className="flex items-center gap-4">
            {activeSystem && (
              <div className="flex items-center gap-1.5">
                <Monitor className="h-3 w-3 text-gray-500" />
                <span className="text-gray-400">{activeSystem.name}</span>
              </div>
            )}
            {license && (
              <div className="flex items-center gap-1.5">
                <KeyRound className="h-3 w-3 text-gray-500" />
                <span className="text-gray-400">{license.client_name}</span>
                <span className="text-gray-600">Opera {license.opera_version}</span>
              </div>
            )}
            <div className="flex items-center gap-1.5">
              <Building2 className="h-3 w-3 text-gray-500" />
              <span className="text-gray-400">{typeof currentCompany === 'object' ? currentCompany?.name : currentCompany || 'Not selected'}</span>
            </div>
          </div>
          <div className="text-gray-600">ESC to go back</div>
        </div>
      </footer>
    </div>
  );
}
