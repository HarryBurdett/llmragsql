import { useState, useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import {
  RotateCcw, FileText, CreditCard, EyeOff, Brain,
  Link2, FileSearch, Trash2, AlertTriangle, CheckCircle, X, Building2
} from 'lucide-react';
import { authFetch } from '../api/client';
import apiClient from '../api/client';
import { PageHeader, LoadingState } from '../components/ui';

interface ResetCard {
  id: string;
  action: string;
  title: string;
  summary: string;
  description: string;
  icon: React.ComponentType<{ className?: string }>;
  tables: string[];
  variant: 'normal' | 'danger';
}

const RESET_CARDS: ResetCard[] = [
  {
    id: 'bank_imports',
    action: 'bank_imports',
    title: 'Bank Import History',
    summary: 'Clear all bank statement import records and transaction lines.',
    description: 'Clears all bank statement import records and transaction lines. Does NOT affect transactions already posted to Opera.',
    icon: FileText,
    tables: ['bank_statement_imports', 'bank_statement_transactions'],
    variant: 'normal',
  },
  {
    id: 'gocardless_imports',
    action: 'gocardless_imports',
    title: 'GoCardless Import History',
    summary: 'Clear all GoCardless payout import records.',
    description: 'Clears all GoCardless payout import records. Does NOT affect transactions already posted to Opera.',
    icon: CreditCard,
    tables: ['gocardless_imports'],
    variant: 'normal',
  },
  {
    id: 'ignored_transactions',
    action: 'ignored_transactions',
    title: 'Ignored Transactions',
    summary: 'Clear all transactions marked as "ignore" during bank imports.',
    description: 'Clears all transactions marked as "ignore" during bank imports. Previously ignored items will reappear as unmatched on future imports.',
    icon: EyeOff,
    tables: ['ignored_bank_transactions'],
    variant: 'normal',
  },
  {
    id: 'learned_patterns',
    action: 'learned_patterns',
    title: 'Learned Patterns',
    summary: 'Clear auto-learned transaction patterns (nominal codes, VAT codes, types).',
    description: 'Clears auto-learned transaction patterns (nominal codes, VAT codes, transaction types). System will re-learn from future imports.',
    icon: Brain,
    tables: ['bank_import_patterns'],
    variant: 'normal',
  },
  {
    id: 'learned_aliases',
    action: 'learned_aliases',
    title: 'Learned Aliases',
    summary: 'Clear auto-learned bank description aliases (customer/supplier mappings).',
    description: 'Clears auto-learned bank description aliases (customer/supplier name mappings). System will re-learn from future imports.',
    icon: Link2,
    tables: ['bank_import_aliases', 'ai_suggestions', 'repeat_entry_aliases'],
    variant: 'normal',
  },
  {
    id: 'pdf_cache',
    action: 'pdf_cache',
    title: 'PDF Extraction Cache',
    summary: 'Clear cached PDF extraction results.',
    description: 'Clears cached PDF extraction results. Statements will be re-extracted from PDF on next import.',
    icon: FileSearch,
    tables: ['extraction_cache'],
    variant: 'normal',
  },
  {
    id: 'full_reset',
    action: 'full_reset',
    title: 'Full Cleardown',
    summary: 'Clear ALL of the above. Use after an Opera database restore.',
    description: 'Clears ALL of the above for the selected company. Use after an Opera database restore to ensure clean state. No Opera data is affected.',
    icon: Trash2,
    tables: [],
    variant: 'danger',
  },
];

function getCardCount(card: ResetCard, counts: Record<string, number>): number {
  if (card.id === 'full_reset') {
    return Object.values(counts).reduce((sum, n) => sum + n, 0);
  }
  return card.tables.reduce((sum, t) => sum + (counts[t] || 0), 0);
}

export function SystemReset() {
  const queryClient = useQueryClient();
  const [selectedCompany, setSelectedCompany] = useState<string>('');
  const [expandedCard, setExpandedCard] = useState<string | null>(null);
  const [executing, setExecuting] = useState<string | null>(null);
  const [result, setResult] = useState<{ action: string; total: number; companyName: string } | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Fetch available companies
  const { data: companiesData } = useQuery({
    queryKey: ['companies'],
    queryFn: async () => {
      const response = await apiClient.getCompanies();
      return response.data;
    },
  });

  const companies = companiesData?.companies || [];
  const currentCompanyId = companiesData?.current_company?.id;

  // Default to current company on first load
  useEffect(() => {
    if (!selectedCompany && currentCompanyId) {
      setSelectedCompany(currentCompanyId);
    }
  }, [currentCompanyId, selectedCompany]);

  const selectedCompanyName = companies.find(c => c.id === selectedCompany)?.name || selectedCompany;

  // Fetch counts for the selected company
  const { data, isLoading } = useQuery({
    queryKey: ['system-reset-counts', selectedCompany],
    queryFn: async () => {
      const url = selectedCompany
        ? `http://localhost:8000/api/admin/system-reset/counts?company_id=${encodeURIComponent(selectedCompany)}`
        : 'http://localhost:8000/api/admin/system-reset/counts';
      const response = await authFetch(url);
      if (!response.ok) {
        const err = await response.json().catch(() => ({ detail: 'Failed to fetch counts' }));
        throw new Error(err.detail || 'Failed to fetch counts');
      }
      return response.json();
    },
    enabled: !!selectedCompany,
  });

  const counts: Record<string, number> = data?.counts || {};

  const handleCompanyChange = (companyId: string) => {
    setSelectedCompany(companyId);
    setExpandedCard(null);
    setResult(null);
    setError(null);
  };

  const handleExecute = async (action: string) => {
    setExecuting(action);
    setError(null);
    setResult(null);

    try {
      const response = await authFetch('http://localhost:8000/api/admin/system-reset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, company_id: selectedCompany || undefined }),
      });

      if (!response.ok) {
        const err = await response.json().catch(() => ({ detail: 'Reset failed' }));
        throw new Error(err.detail || 'Reset failed');
      }

      const data = await response.json();
      setResult({ action, total: data.total_deleted, companyName: selectedCompanyName });
      setExpandedCard(null);
      queryClient.invalidateQueries({ queryKey: ['system-reset-counts', selectedCompany] });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'An unexpected error occurred');
    } finally {
      setExecuting(null);
    }
  };

  if (!selectedCompany) {
    return (
      <div className="p-6">
        <PageHeader icon={RotateCcw} title="Cashbook Routines Cleardown" subtitle="Clear application data and caches" />
        <LoadingState message="Loading companies..." />
      </div>
    );
  }

  return (
    <div className="p-6 max-w-5xl">
      <PageHeader icon={RotateCcw} title="Cashbook Routines Cleardown" subtitle="Clear application data and caches. Opera transactions are never affected." />

      {/* Company selector */}
      <div className="mt-4 flex items-center gap-3">
        <Building2 className="w-5 h-5 text-gray-400" />
        <label className="text-sm font-medium text-gray-700">Company:</label>
        <select
          value={selectedCompany}
          onChange={(e) => handleCompanyChange(e.target.value)}
          disabled={!!executing}
          className="px-3 py-1.5 text-sm border border-gray-300 rounded-md bg-white focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:opacity-50"
        >
          {companies.map((c) => (
            <option key={c.id} value={c.id}>
              {c.name}{c.id === currentCompanyId ? ' (active)' : ''}
            </option>
          ))}
        </select>
        {selectedCompany !== currentCompanyId && (
          <span className="text-xs text-amber-600 bg-amber-50 px-2 py-1 rounded">
            Not the active company
          </span>
        )}
      </div>

      {/* Success feedback */}
      {result && (
        <div className="mt-4 p-3 bg-green-50 border border-green-200 rounded-lg flex items-start gap-2">
          <CheckCircle className="w-5 h-5 text-green-500 mt-0.5 flex-shrink-0" />
          <div className="flex-1">
            <p className="text-sm text-green-800 font-medium">Reset complete — {result.companyName}</p>
            <p className="text-sm text-green-700">{result.total.toLocaleString()} record{result.total !== 1 ? 's' : ''} deleted.</p>
          </div>
          <button onClick={() => setResult(null)} className="text-green-400 hover:text-green-600">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Error feedback */}
      {error && (
        <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
          <AlertTriangle className="w-5 h-5 text-red-500 mt-0.5 flex-shrink-0" />
          <div className="flex-1">
            <p className="text-sm text-red-800 font-medium">Reset failed</p>
            <p className="text-sm text-red-700">{error}</p>
          </div>
          <button onClick={() => setError(null)} className="text-red-400 hover:text-red-600">
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Loading counts */}
      {isLoading ? (
        <div className="mt-6"><LoadingState message="Loading record counts..." /></div>
      ) : (
        /* Cards grid */
        <div className="mt-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {RESET_CARDS.map((card) => {
            const Icon = card.icon;
            const count = getCardCount(card, counts);
            const isExpanded = expandedCard === card.id;
            const isExecuting = executing === card.id;
            const isDanger = card.variant === 'danger';

            return (
              <div
                key={card.id}
                className={`rounded-lg border transition-all ${
                  isExpanded
                    ? isDanger
                      ? 'border-red-300 bg-red-50 col-span-1 md:col-span-2 lg:col-span-3'
                      : 'border-blue-300 bg-blue-50 col-span-1 md:col-span-2 lg:col-span-3'
                    : isDanger
                      ? 'border-red-200 bg-white hover:border-red-300 hover:shadow-sm cursor-pointer'
                      : 'border-gray-200 bg-white hover:border-blue-300 hover:shadow-sm cursor-pointer'
                }`}
                onClick={() => {
                  if (!isExpanded && !executing) {
                    setExpandedCard(card.id);
                    setResult(null);
                    setError(null);
                  }
                }}
              >
                {/* Card header */}
                <div className="p-4">
                  <div className="flex items-start gap-3">
                    <div className={`p-2 rounded-lg ${isDanger ? 'bg-red-100' : 'bg-gray-100'}`}>
                      <Icon className={`w-5 h-5 ${isDanger ? 'text-red-600' : 'text-gray-600'}`} />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2">
                        <h3 className={`font-medium ${isDanger ? 'text-red-900' : 'text-gray-900'}`}>{card.title}</h3>
                        {count > 0 && (
                          <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${
                            isDanger ? 'bg-red-200 text-red-800' : 'bg-gray-200 text-gray-700'
                          }`}>
                            {count.toLocaleString()}
                          </span>
                        )}
                      </div>
                      <p className="text-sm text-gray-500 mt-0.5">{card.summary}</p>
                    </div>
                  </div>
                </div>

                {/* Expanded detail */}
                {isExpanded && (
                  <div className="px-4 pb-4 border-t border-gray-200 pt-3" onClick={(e) => e.stopPropagation()}>
                    <p className="text-sm text-gray-700">{card.description}</p>

                    {card.tables.length > 0 && (
                      <div className="mt-2">
                        <p className="text-xs text-gray-500 font-medium">Tables to clear:</p>
                        <div className="flex flex-wrap gap-1 mt-1">
                          {card.tables.map((t) => (
                            <span key={t} className="text-xs bg-gray-100 text-gray-600 px-2 py-0.5 rounded font-mono">
                              {t} ({(counts[t] || 0).toLocaleString()})
                            </span>
                          ))}
                        </div>
                      </div>
                    )}

                    <div className="mt-3 flex items-center gap-2 p-2 bg-amber-50 border border-amber-200 rounded-md">
                      <AlertTriangle className="w-4 h-4 text-amber-600 flex-shrink-0" />
                      <p className="text-xs text-amber-800">This cannot be undone. {count.toLocaleString()} record{count !== 1 ? 's' : ''} will be permanently deleted from <strong>{selectedCompanyName}</strong>.</p>
                    </div>

                    <div className="mt-3 flex gap-2">
                      <button
                        onClick={() => handleExecute(card.action)}
                        disabled={isExecuting || count === 0}
                        className={`px-4 py-2 text-sm font-medium rounded-md text-white transition-colors ${
                          isExecuting || count === 0
                            ? 'bg-gray-300 cursor-not-allowed'
                            : 'bg-red-600 hover:bg-red-700'
                        }`}
                      >
                        {isExecuting ? 'Clearing...' : 'OK to Proceed'}
                      </button>
                      <button
                        onClick={() => setExpandedCard(null)}
                        disabled={isExecuting}
                        className="px-4 py-2 text-sm font-medium rounded-md text-gray-700 bg-gray-100 hover:bg-gray-200 transition-colors"
                      >
                        Cancel
                      </button>
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
