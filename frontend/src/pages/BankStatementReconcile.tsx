import { useState, useMemo, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  CheckCircle,
  RefreshCw,
  Landmark,
  CheckSquare,
  Square,
  ArrowUpDown,
  Search,
  Upload,
  FileText,
  AlertCircle,
  Check,
  X,
  FolderOpen,
  ChevronDown,
} from 'lucide-react';
import apiClient from '../api/client';
import type {
  BankAccountsResponse,
  BankReconciliationStatusResponse,
  UnreconciledEntriesResponse,
  MarkReconciledResponse,
} from '../api/client';

interface BankValidation {
  valid: boolean;
  match_type?: string;
  error?: string;
  opera_bank?: {
    code: string;
    description: string;
    sort_code: string;
    account_number: string;
  };
  suggested_bank?: {
    bank_code: string;
    description: string;
  };
}

interface StatementMatch {
  statement_txn: {
    date: string;
    description: string;
    amount: number;
    balance: number | null;
    type: string | null;
  };
  opera_entry: {
    ae_entry: string;
    ae_date: string;
    ae_ref: string;
    value_pounds: number;
    ae_detail: string;
  };
  match_score: number;
  match_reasons: string[];
}

interface StatementTransaction {
  date: string;
  description: string;
  amount: number;
  balance: number | null;
  type: string | null;
}

interface ProcessStatementResponse {
  success: boolean;
  error?: string;
  bank_code?: string;
  bank_validation?: BankValidation;
  statement_info?: {
    bank_name: string;
    account_number: string;
    sort_code: string | null;
    statement_date: string | null;
    period_start: string | null;
    period_end: string | null;
    opening_balance: number | null;
    closing_balance: number | null;
  };
  extracted_transactions?: number;
  opera_unreconciled?: number;
  matches?: StatementMatch[];
  unmatched_statement?: StatementTransaction[];
  unmatched_opera?: {
    ae_entry: string;
    ae_date: string;
    ae_ref: string;
    value_pounds: number;
    ae_detail: string;
  }[];
}

interface UnifiedStatementResponse {
  success: boolean;
  error?: string;
  statement_info?: {
    bank_name: string;
    account_number: string;
    sort_code: string | null;
    statement_date: string | null;
    period_start: string | null;
    period_end: string | null;
    opening_balance: number | null;
    closing_balance: number | null;
  };
  summary?: {
    total_statement_txns: number;
    to_import: number;
    to_reconcile: number;
    already_reconciled: number;
    opera_entries_in_period: number;
  };
  to_import?: StatementTransaction[];
  to_reconcile?: StatementMatch[];
  already_reconciled?: StatementMatch[];
  balance_check?: {
    statement_closing: number | null;
    statement_opening: number | null;
    opera_current_balance: number;
    opera_reconciled_balance: number;
    import_total: number;
    expected_after_import: number;
    variance: number | null;
  };
}

type AutoTab = 'import' | 'reconcile' | 'verified';

type ViewMode = 'manual' | 'auto';

interface StatementFile {
  path: string;
  filename: string;
  folder: string;
  size: number;
  size_formatted: string;
  modified: string;
  modified_formatted: string;
}

interface StatementFilesResponse {
  success: boolean;
  files: StatementFile[];
  count: number;
  error?: string;
}

export function BankStatementReconcile() {
  const queryClient = useQueryClient();
  const [viewMode, setViewMode] = useState<ViewMode>('manual');
  const [selectedBank, setSelectedBank] = useState<string>('BC010');
  const [selectedEntries, setSelectedEntries] = useState<Set<string>>(new Set());
  const [statementNumber, setStatementNumber] = useState<string>('');
  const [statementDate, setStatementDate] = useState<string>(
    new Date().toISOString().split('T')[0]
  );
  const [statementBalance, setStatementBalance] = useState<string>('0.00');
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [sortField, setSortField] = useState<'ae_entry' | 'value_pounds' | 'ae_lstdate'>('ae_lstdate');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');

  // Auto-match state - load last used path for the selected bank from localStorage
  const getStoredPath = (bankCode: string) => {
    const saved = localStorage.getItem(`statementPath_${bankCode}`);
    return saved || '/Users/maccb/Downloads/bank-statements/';
  };

  const [statementPath, setStatementPath] = useState<string>(() => getStoredPath('BC010'));
  const [statementResult, setStatementResult] = useState<ProcessStatementResponse | null>(null);
  const [selectedMatches, setSelectedMatches] = useState<Set<number>>(new Set());
  const [isProcessing, setIsProcessing] = useState(false);
  const [useManualPath, setUseManualPath] = useState(false);
  const [selectedFile, setSelectedFile] = useState<string>('');

  // Fetch available statement files
  const statementFilesQuery = useQuery<StatementFilesResponse>({
    queryKey: ['statementFiles'],
    queryFn: async () => {
      const response = await fetch('/api/statement-files');
      return response.json();
    },
    staleTime: 30000, // Cache for 30 seconds
  });

  // Update statementPath when a file is selected from the dropdown
  useEffect(() => {
    if (selectedFile) {
      setStatementPath(selectedFile);
    }
  }, [selectedFile]);

  // Save path to localStorage for the current bank when processing succeeds
  const savePathToHistory = (path: string, bankCode: string) => {
    localStorage.setItem(`statementPath_${bankCode}`, path);
  };

  // Load stored path when bank selection changes
  const handleBankChange = (newBank: string) => {
    setSelectedBank(newBank);
    setStatementPath(getStoredPath(newBank));
    setStatementResult(null); // Clear previous results
  };

  // Fetch bank accounts
  const banksQuery = useQuery<BankAccountsResponse>({
    queryKey: ['reconcileBanks'],
    queryFn: async () => {
      const response = await apiClient.reconcileBanks();
      return response.data;
    },
  });

  // Fetch reconciliation status
  const statusQuery = useQuery<BankReconciliationStatusResponse>({
    queryKey: ['bankRecStatus', selectedBank],
    queryFn: async () => {
      const response = await apiClient.getBankReconciliationStatus(selectedBank);
      return response.data;
    },
    enabled: !!selectedBank,
  });

  // Fetch unreconciled entries
  const entriesQuery = useQuery<UnreconciledEntriesResponse>({
    queryKey: ['unreconciledEntries', selectedBank],
    queryFn: async () => {
      const response = await apiClient.getUnreconciledEntries(selectedBank);
      return response.data;
    },
    enabled: !!selectedBank,
  });

  // Mark reconciled mutation (manual mode)
  const markReconciledMutation = useMutation<MarkReconciledResponse, Error, void>({
    mutationFn: async () => {
      const entries = Array.from(selectedEntries).map((entry, index) => ({
        entry_number: entry,
        statement_line: (index + 1) * 10,
      }));
      const response = await apiClient.markEntriesReconciled(selectedBank, {
        entries,
        statement_number: parseInt(statementNumber) || (statusQuery.data?.last_stmt_no || 0) + 1,
        statement_date: statementDate,
        reconciliation_date: statementDate,
      });
      return response.data;
    },
    onSuccess: () => {
      setSelectedEntries(new Set());
      queryClient.invalidateQueries({ queryKey: ['bankRecStatus', selectedBank] });
      queryClient.invalidateQueries({ queryKey: ['unreconciledEntries', selectedBank] });
    },
  });

  // Process statement
  const processStatement = async () => {
    if (!statementPath.trim()) {
      alert('Please enter a statement file path');
      return;
    }

    setIsProcessing(true);
    setStatementResult(null);

    try {
      const response = await fetch(
        `/api/reconcile/process-statement?file_path=${encodeURIComponent(statementPath)}&bank_code=${encodeURIComponent(selectedBank)}`,
        { method: 'POST' }
      );
      const data: ProcessStatementResponse = await response.json();

      if (data.success) {
        setStatementResult(data);
        // Save successful path to history for this bank
        savePathToHistory(statementPath, selectedBank);
        // Pre-select all matches
        if (data.matches) {
          setSelectedMatches(new Set(data.matches.map((_, i) => i)));
        }
        // Update statement balance and date from extracted data
        if (data.statement_info?.closing_balance != null) {
          setStatementBalance(data.statement_info.closing_balance.toString());
        }
        if (data.statement_info?.period_end) {
          setStatementDate(data.statement_info.period_end.split('T')[0]);
        }
      } else {
        // Check if it's a bank mismatch error with suggestion
        if (data.bank_validation?.suggested_bank) {
          const suggested = data.bank_validation.suggested_bank;
          const useOther = window.confirm(
            `${data.error}\n\nWould you like to switch to bank account '${suggested.bank_code}' (${suggested.description})?`
          );
          if (useOther) {
            handleBankChange(suggested.bank_code);
          }
        } else {
          alert(`Error: ${data.error}`);
        }
      }
    } catch (error) {
      alert(`Failed to process statement: ${error}`);
    } finally {
      setIsProcessing(false);
    }
  };

  // Confirm auto-matches
  const confirmMatches = async () => {
    if (!statementResult?.matches || selectedMatches.size === 0) return;

    const matchesToConfirm = statementResult.matches.filter((_, i) => selectedMatches.has(i));

    try {
      const response = await fetch(
        `/api/reconcile/bank/${selectedBank}/confirm-matches`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            matches: matchesToConfirm.map(m => ({ ae_entry: m.opera_entry.ae_entry })),
            statement_balance: parseFloat(statementBalance),
            statement_date: statementDate,
          }),
        }
      );
      const data = await response.json();

      if (data.success) {
        // Prompt to archive the statement file
        const shouldArchive = window.confirm(
          `Successfully reconciled ${data.reconciled_count} entries.\n\nArchive this statement file?`
        );

        if (shouldArchive && statementPath) {
          try {
            const archiveResponse = await fetch(
              `/api/archive/file?file_path=${encodeURIComponent(statementPath)}&import_type=bank-statement&transactions_extracted=${statementResult?.extracted_transactions || 0}&transactions_matched=${statementResult?.matches?.length || 0}&transactions_reconciled=${data.reconciled_count}`,
              { method: 'POST' }
            );
            const archiveData = await archiveResponse.json();
            if (archiveData.success) {
              alert(`Statement archived to:\n${archiveData.archive_path}`);
            }
          } catch {
            // Silently fail archive - main operation succeeded
          }
        }

        setStatementResult(null);
        setSelectedMatches(new Set());
        queryClient.invalidateQueries({ queryKey: ['bankRecStatus', selectedBank] });
        queryClient.invalidateQueries({ queryKey: ['unreconciledEntries', selectedBank] });
      } else {
        alert(`Error: ${data.error}`);
      }
    } catch (error) {
      alert(`Failed to confirm matches: ${error}`);
    }
  };

  const formatCurrency = (value: number | undefined | null) => {
    if (value === undefined || value === null) return '';
    return Math.abs(value).toLocaleString('en-GB', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-GB');
  };

  const toggleEntry = (entryNumber: string) => {
    const newSelected = new Set(selectedEntries);
    if (newSelected.has(entryNumber)) {
      newSelected.delete(entryNumber);
    } else {
      newSelected.add(entryNumber);
    }
    setSelectedEntries(newSelected);
  };

  const toggleAll = () => {
    if (filteredEntries.length === selectedEntries.size) {
      setSelectedEntries(new Set());
    } else {
      setSelectedEntries(new Set(filteredEntries.map(e => e.ae_entry)));
    }
  };

  const handleSort = (field: 'ae_entry' | 'value_pounds' | 'ae_lstdate') => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('asc');
    }
  };

  // Filter and sort entries
  const filteredEntries = useMemo(() => {
    let entries = entriesQuery.data?.entries || [];

    if (searchTerm) {
      const term = searchTerm.toLowerCase();
      entries = entries.filter(
        e =>
          e.ae_entry.toLowerCase().includes(term) ||
          e.ae_entref?.toLowerCase().includes(term) ||
          e.ae_comment?.toLowerCase().includes(term)
      );
    }

    entries = [...entries].sort((a, b) => {
      let aVal: string | number = a[sortField] ?? '';
      let bVal: string | number = b[sortField] ?? '';

      if (sortField === 'value_pounds') {
        aVal = a.value_pounds;
        bVal = b.value_pounds;
      } else if (sortField === 'ae_lstdate') {
        aVal = a.ae_lstdate || '';
        bVal = b.ae_lstdate || '';
      }

      if (typeof aVal === 'string') {
        aVal = aVal.toLowerCase();
        bVal = (bVal as string).toLowerCase();
      }

      if (sortDirection === 'asc') {
        return aVal > bVal ? 1 : -1;
      } else {
        return aVal < bVal ? 1 : -1;
      }
    });

    return entries;
  }, [entriesQuery.data?.entries, searchTerm, sortField, sortDirection]);

  // Calculate totals
  const totals = useMemo(() => {
    const selected = filteredEntries.filter(e => selectedEntries.has(e.ae_entry));
    const reconciled = selected.reduce((sum, e) => sum + e.value_pounds, 0);
    const stmtBal = parseFloat(statementBalance) || 0;
    const difference = stmtBal - reconciled;

    return { reconciled, statementBalance: stmtBal, difference };
  }, [filteredEntries, selectedEntries, statementBalance]);

  // Calculate running balance for display
  const entriesWithBalance = useMemo(() => {
    let runningBalance = statusQuery.data?.reconciled_balance || 0;
    return filteredEntries.map((entry) => {
      if (selectedEntries.has(entry.ae_entry)) {
        runningBalance += entry.value_pounds;
      }
      return {
        ...entry,
        runningBalance,
        lineNumber: selectedEntries.has(entry.ae_entry)
          ? (Array.from(selectedEntries).indexOf(entry.ae_entry) + 1) * 10
          : null,
      };
    });
  }, [filteredEntries, selectedEntries, statusQuery.data?.reconciled_balance]);

  const bankDescription = banksQuery.data?.banks?.find(b => b.account_code === selectedBank)?.description || '';

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="mb-4 flex justify-between items-center">
        <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
          <Landmark className="w-6 h-6 text-blue-600" />
          Reconcile: {bankDescription || selectedBank}
        </h1>

        {/* Mode Toggle */}
        <div className="flex bg-gray-200 rounded-lg p-1">
          <button
            onClick={() => setViewMode('manual')}
            className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
              viewMode === 'manual'
                ? 'bg-white text-blue-600 shadow'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            Manual
          </button>
          <button
            onClick={() => setViewMode('auto')}
            className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
              viewMode === 'auto'
                ? 'bg-white text-blue-600 shadow'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            Auto-Match
          </button>
        </div>
      </div>

      {/* Bank Selector Row */}
      <div className="bg-gray-100 border border-gray-300 rounded p-3 mb-4">
        <div className="flex items-center gap-6 flex-wrap">
          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">Bank:</label>
            <select
              value={selectedBank}
              onChange={e => {
                handleBankChange(e.target.value);
                setSelectedEntries(new Set());
              }}
              className="border border-gray-400 rounded px-2 py-1 min-w-[250px] bg-white"
            >
              {banksQuery.data?.banks?.map(bank => (
                <option key={bank.account_code} value={bank.account_code}>
                  {bank.description || bank.account_code}
                </option>
              ))}
            </select>
          </div>

          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">Statement Date:</label>
            <input
              type="date"
              value={statementDate}
              onChange={e => setStatementDate(e.target.value)}
              className="border border-gray-400 rounded px-2 py-1 bg-white"
            />
          </div>

          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">Statement Balance:</label>
            <input
              type="number"
              step="0.01"
              value={statementBalance}
              onChange={e => setStatementBalance(e.target.value)}
              className="border border-gray-400 rounded px-2 py-1 w-32 bg-white text-right"
            />
          </div>

          <button
            onClick={() => {
              statusQuery.refetch();
              entriesQuery.refetch();
            }}
            className="p-1 text-gray-600 hover:text-gray-800 hover:bg-gray-200 rounded"
            title="Refresh"
          >
            <RefreshCw className={`w-4 h-4 ${statusQuery.isFetching ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {viewMode === 'auto' ? (
        /* ==================== AUTO-MATCH MODE ==================== */
        <div>
          {/* Statement Upload Section */}
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-4">
            <div className="flex items-center gap-2 mb-3">
              <Upload className="w-5 h-5 text-blue-600" />
              <h2 className="font-medium text-blue-900">Process Bank Statement</h2>
            </div>

            <div className="flex gap-3 items-end">
              <div className="flex-1">
                <div className="flex items-center justify-between mb-1">
                  <label className="text-sm text-gray-600">Statement File (PDF)</label>
                  <button
                    onClick={() => {
                      setUseManualPath(!useManualPath);
                      if (!useManualPath) {
                        setSelectedFile('');
                      }
                    }}
                    className="text-xs text-blue-600 hover:text-blue-800"
                  >
                    {useManualPath ? 'Browse files' : 'Enter path manually'}
                  </button>
                </div>

                {useManualPath ? (
                  <input
                    type="text"
                    value={statementPath}
                    onChange={e => setStatementPath(e.target.value)}
                    placeholder="/Users/maccb/Downloads/Statement.pdf"
                    className="w-full border border-gray-300 rounded px-3 py-2"
                  />
                ) : (
                  <div className="relative">
                    <select
                      value={selectedFile}
                      onChange={e => setSelectedFile(e.target.value)}
                      className="w-full border border-gray-300 rounded px-3 py-2 pr-8 appearance-none bg-white"
                    >
                      <option value="">-- Select a statement file --</option>
                      {statementFilesQuery.data?.files?.map(file => (
                        <option key={file.path} value={file.path}>
                          [{file.folder}] {file.filename} ({file.modified_formatted})
                        </option>
                      ))}
                    </select>
                    <ChevronDown className="w-4 h-4 absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
                    {statementFilesQuery.data?.count === 0 && (
                      <p className="text-xs text-amber-600 mt-1">
                        No PDF files found. Use "Enter path manually" or add files to bank-statements folders.
                      </p>
                    )}
                  </div>
                )}
              </div>

              <button
                onClick={() => statementFilesQuery.refetch()}
                className="p-2 text-gray-600 hover:text-gray-800 hover:bg-gray-200 rounded"
                title="Refresh file list"
              >
                <FolderOpen className={`w-5 h-5 ${statementFilesQuery.isFetching ? 'animate-pulse' : ''}`} />
              </button>

              <button
                onClick={processStatement}
                disabled={isProcessing || !statementPath.trim()}
                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2"
              >
                {isProcessing ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <FileText className="w-4 h-4" />
                )}
                {isProcessing ? 'Processing...' : 'Process Statement'}
              </button>
            </div>
          </div>

          {/* Statement Results */}
          {statementResult && (
            <div className="space-y-4">
              {/* Statement Info */}
              {statementResult.statement_info && (
                <div className="bg-white border border-gray-200 rounded-lg p-4">
                  <h3 className="font-medium mb-2 flex items-center gap-2">
                    <FileText className="w-4 h-4" />
                    Statement: {statementResult.statement_info.bank_name}
                  </h3>
                  <div className="grid grid-cols-4 gap-4 text-sm">
                    <div>
                      <span className="text-gray-500">Account:</span>{' '}
                      <span className="font-medium">{statementResult.statement_info.account_number}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Period:</span>{' '}
                      <span className="font-medium">
                        {formatDate(statementResult.statement_info.period_start)} - {formatDate(statementResult.statement_info.period_end)}
                      </span>
                    </div>
                    <div>
                      <span className="text-gray-500">Opening:</span>{' '}
                      <span className="font-medium">{formatCurrency(statementResult.statement_info.opening_balance)}</span>
                    </div>
                    <div>
                      <span className="text-gray-500">Closing:</span>{' '}
                      <span className="font-medium">{formatCurrency(statementResult.statement_info.closing_balance)}</span>
                    </div>
                  </div>
                </div>
              )}

              {/* Summary Stats */}
              <div className="grid grid-cols-4 gap-4">
                <div className="bg-white border border-gray-200 rounded-lg p-4 text-center">
                  <div className="text-2xl font-bold text-gray-900">{statementResult.extracted_transactions}</div>
                  <div className="text-sm text-gray-500">Statement Transactions</div>
                </div>
                <div className="bg-white border border-gray-200 rounded-lg p-4 text-center">
                  <div className="text-2xl font-bold text-gray-900">{statementResult.opera_unreconciled}</div>
                  <div className="text-sm text-gray-500">Opera Unreconciled</div>
                </div>
                <div className="bg-green-50 border border-green-200 rounded-lg p-4 text-center">
                  <div className="text-2xl font-bold text-green-600">{statementResult.matches?.length || 0}</div>
                  <div className="text-sm text-green-700">Matched</div>
                </div>
                <div className="bg-orange-50 border border-orange-200 rounded-lg p-4 text-center">
                  <div className="text-2xl font-bold text-orange-600">
                    {(statementResult.unmatched_statement?.length || 0) + (statementResult.unmatched_opera?.length || 0)}
                  </div>
                  <div className="text-sm text-orange-700">Unmatched</div>
                </div>
              </div>

              {/* Matched Transactions */}
              {statementResult.matches && statementResult.matches.length > 0 && (
                <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
                  <div className="bg-green-50 px-4 py-3 border-b border-green-200 flex justify-between items-center">
                    <h3 className="font-medium text-green-900 flex items-center gap-2">
                      <CheckCircle className="w-4 h-4" />
                      Matched Transactions ({selectedMatches.size}/{statementResult.matches.length} selected)
                    </h3>
                    <div className="flex gap-2">
                      <button
                        onClick={() => setSelectedMatches(new Set(statementResult.matches!.map((_, i) => i)))}
                        className="text-sm text-green-700 hover:text-green-900"
                      >
                        Select All
                      </button>
                      <span className="text-gray-300">|</span>
                      <button
                        onClick={() => setSelectedMatches(new Set())}
                        className="text-sm text-green-700 hover:text-green-900"
                      >
                        Select None
                      </button>
                    </div>
                  </div>
                  <div className="max-h-96 overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          <th className="w-8 px-2 py-2"></th>
                          <th className="px-3 py-2 text-left">Statement</th>
                          <th className="px-3 py-2 text-left">Opera Entry</th>
                          <th className="px-3 py-2 text-right">Amount</th>
                          <th className="px-3 py-2 text-center">Score</th>
                        </tr>
                      </thead>
                      <tbody>
                        {statementResult.matches.map((match, idx) => (
                          <tr
                            key={idx}
                            className={`border-t cursor-pointer hover:bg-gray-50 ${
                              selectedMatches.has(idx) ? 'bg-green-50' : ''
                            }`}
                            onClick={() => {
                              const newSelected = new Set(selectedMatches);
                              if (newSelected.has(idx)) {
                                newSelected.delete(idx);
                              } else {
                                newSelected.add(idx);
                              }
                              setSelectedMatches(newSelected);
                            }}
                          >
                            <td className="px-2 py-2 text-center">
                              <input
                                type="checkbox"
                                checked={selectedMatches.has(idx)}
                                onChange={() => {}}
                                className="rounded"
                              />
                            </td>
                            <td className="px-3 py-2">
                              <div className="font-medium">{formatDate(match.statement_txn.date)}</div>
                              <div className="text-xs text-gray-500 truncate max-w-xs" title={match.statement_txn.description}>
                                {match.statement_txn.description}
                              </div>
                            </td>
                            <td className="px-3 py-2">
                              <div className="font-medium">{match.opera_entry.ae_ref}</div>
                              <div className="text-xs text-gray-500">{formatDate(match.opera_entry.ae_date)}</div>
                            </td>
                            <td className={`px-3 py-2 text-right font-medium ${match.statement_txn.amount < 0 ? 'text-red-600' : 'text-green-600'}`}>
                              {match.statement_txn.amount < 0 ? '-' : '+'}
                              {formatCurrency(match.statement_txn.amount)}
                            </td>
                            <td className="px-3 py-2 text-center">
                              <span className={`px-2 py-0.5 rounded text-xs font-medium ${
                                match.match_score >= 0.9 ? 'bg-green-100 text-green-800' :
                                match.match_score >= 0.8 ? 'bg-yellow-100 text-yellow-800' :
                                'bg-orange-100 text-orange-800'
                              }`}>
                                {Math.round(match.match_score * 100)}%
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Unmatched Statement Transactions */}
              {statementResult.unmatched_statement && statementResult.unmatched_statement.length > 0 && (
                <div className="bg-white border border-orange-200 rounded-lg overflow-hidden">
                  <div className="bg-orange-50 px-4 py-3 border-b border-orange-200">
                    <h3 className="font-medium text-orange-900 flex items-center gap-2">
                      <AlertCircle className="w-4 h-4" />
                      Unmatched Statement Transactions ({statementResult.unmatched_statement.length})
                    </h3>
                  </div>
                  <div className="max-h-48 overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          <th className="px-3 py-2 text-left">Date</th>
                          <th className="px-3 py-2 text-left">Description</th>
                          <th className="px-3 py-2 text-right">Amount</th>
                        </tr>
                      </thead>
                      <tbody>
                        {statementResult.unmatched_statement.map((txn, idx) => (
                          <tr key={idx} className="border-t">
                            <td className="px-3 py-2">{formatDate(txn.date)}</td>
                            <td className="px-3 py-2 text-gray-600 truncate max-w-md" title={txn.description}>
                              {txn.description}
                            </td>
                            <td className={`px-3 py-2 text-right ${txn.amount < 0 ? 'text-red-600' : 'text-green-600'}`}>
                              {txn.amount < 0 ? '-' : '+'}
                              {formatCurrency(txn.amount)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Action Buttons */}
              <div className="flex justify-end gap-3">
                <button
                  onClick={() => {
                    setStatementResult(null);
                    setSelectedMatches(new Set());
                  }}
                  className="px-4 py-2 border border-gray-300 rounded hover:bg-gray-50 flex items-center gap-2"
                >
                  <X className="w-4 h-4" />
                  Cancel
                </button>
                <button
                  onClick={confirmMatches}
                  disabled={selectedMatches.size === 0}
                  className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
                >
                  <Check className="w-4 h-4" />
                  Reconcile {selectedMatches.size} Matches
                </button>
              </div>
            </div>
          )}

          {/* No results message */}
          {!statementResult && !isProcessing && (
            <div className="bg-white border border-gray-200 rounded-lg p-8 text-center text-gray-500">
              <FileText className="w-12 h-12 mx-auto mb-3 text-gray-300" />
              <p>Enter a bank statement file path and click "Process Statement" to auto-match transactions</p>
            </div>
          )}
        </div>
      ) : (
        /* ==================== MANUAL MODE ==================== */
        <div>
          {/* Search */}
          <div className="mb-2 flex items-center gap-4">
            <div className="flex items-center gap-2">
              <label className="text-sm font-medium text-gray-700">Statement #:</label>
              <input
                type="number"
                value={statementNumber}
                onChange={e => setStatementNumber(e.target.value)}
                placeholder={String((statusQuery.data?.last_stmt_no || 0) + 1)}
                className="border border-gray-400 rounded px-2 py-1 w-24 bg-white"
              />
            </div>
            <div className="relative">
              <Search className="w-4 h-4 absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Search..."
                value={searchTerm}
                onChange={e => setSearchTerm(e.target.value)}
                className="pl-8 pr-3 py-1 border border-gray-400 rounded w-48 text-sm"
              />
            </div>
            <button
              onClick={toggleAll}
              className="px-2 py-1 text-xs border border-gray-400 rounded hover:bg-gray-100 flex items-center gap-1"
            >
              {filteredEntries.length === selectedEntries.size ? (
                <CheckSquare className="w-3 h-3" />
              ) : (
                <Square className="w-3 h-3" />
              )}
              {filteredEntries.length === selectedEntries.size ? 'Untick All' : 'Tick All'}
            </button>
          </div>

          {/* Entries Table */}
          <div className="border border-gray-400 bg-white">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-200 border-b border-gray-400">
                  <th className="w-8 px-1 py-2 text-center border-r border-gray-300"></th>
                  <th
                    className="px-2 py-2 text-left font-medium text-gray-700 border-r border-gray-300 cursor-pointer hover:bg-gray-300"
                    onClick={() => handleSort('ae_lstdate')}
                  >
                    <div className="flex items-center gap-1">
                      Date
                      <ArrowUpDown className="w-3 h-3" />
                    </div>
                  </th>
                  <th
                    className="px-2 py-2 text-left font-medium text-gray-700 border-r border-gray-300 cursor-pointer hover:bg-gray-300"
                    onClick={() => handleSort('ae_entry')}
                  >
                    <div className="flex items-center gap-1">
                      Reference
                      <ArrowUpDown className="w-3 h-3" />
                    </div>
                  </th>
                  <th className="px-2 py-2 text-left font-medium text-gray-700 border-r border-gray-300">
                    Cashbook Type
                  </th>
                  <th
                    className="px-2 py-2 text-right font-medium text-gray-700 border-r border-gray-300 cursor-pointer hover:bg-gray-300"
                    onClick={() => handleSort('value_pounds')}
                  >
                    <div className="flex items-center justify-end gap-1">
                      Payments
                      <ArrowUpDown className="w-3 h-3" />
                    </div>
                  </th>
                  <th className="px-2 py-2 text-right font-medium text-gray-700 border-r border-gray-300">
                    Receipts
                  </th>
                  <th className="px-2 py-2 text-right font-medium text-gray-700 border-r border-gray-300">
                    Balance
                  </th>
                  <th className="px-2 py-2 text-right font-medium text-gray-700 w-16">
                    Line
                  </th>
                </tr>
              </thead>
              <tbody>
                {entriesQuery.isLoading ? (
                  <tr>
                    <td colSpan={8} className="px-4 py-8 text-center text-gray-500">
                      <RefreshCw className="w-5 h-5 animate-spin mx-auto mb-2" />
                      Loading...
                    </td>
                  </tr>
                ) : filteredEntries.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="px-4 py-8 text-center text-gray-500">
                      <CheckCircle className="w-5 h-5 mx-auto mb-2 text-green-500" />
                      No unreconciled entries
                    </td>
                  </tr>
                ) : (
                  entriesWithBalance.map((entry, idx) => {
                    const isSelected = selectedEntries.has(entry.ae_entry);
                    const isPayment = entry.value_pounds < 0;

                    return (
                      <tr
                        key={entry.ae_entry}
                        className={`border-b border-gray-200 cursor-pointer ${
                          isSelected ? 'bg-blue-100' : idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'
                        } hover:bg-blue-50`}
                        onClick={() => toggleEntry(entry.ae_entry)}
                      >
                        <td className="px-1 py-1 text-center border-r border-gray-200">
                          <input
                            type="checkbox"
                            checked={isSelected}
                            onChange={() => toggleEntry(entry.ae_entry)}
                            onClick={e => e.stopPropagation()}
                            className="rounded border-gray-400"
                          />
                        </td>
                        <td className="px-2 py-1 border-r border-gray-200 text-gray-700">
                          {formatDate(entry.ae_lstdate)}
                        </td>
                        <td className="px-2 py-1 border-r border-gray-200 text-gray-900">
                          {entry.ae_entref?.trim() || entry.ae_entry}
                        </td>
                        <td className="px-2 py-1 border-r border-gray-200 text-gray-700">
                          {entry.ae_cbtype?.trim() || '-'}
                        </td>
                        <td className="px-2 py-1 border-r border-gray-200 text-right text-gray-900">
                          {isPayment ? formatCurrency(entry.value_pounds) : ''}
                        </td>
                        <td className="px-2 py-1 border-r border-gray-200 text-right text-gray-900">
                          {!isPayment ? formatCurrency(entry.value_pounds) : ''}
                        </td>
                        <td className="px-2 py-1 border-r border-gray-200 text-right text-gray-900">
                          {isSelected ? formatCurrency(entry.runningBalance) : ''}
                        </td>
                        <td className="px-2 py-1 text-right text-gray-900">
                          {entry.lineNumber || ''}
                        </td>
                      </tr>
                    );
                  })
                )}
              </tbody>
            </table>
          </div>

          {/* Footer */}
          <div className="bg-gray-100 border border-t-0 border-gray-400 px-4 py-3 flex justify-between items-center">
            <div className="flex gap-8">
              <div>
                <span className="text-sm text-gray-600">Statement Balance: </span>
                <span className="font-medium">{formatCurrency(totals.statementBalance)}</span>
              </div>
              <div>
                <span className="text-sm text-gray-600">Reconciled: </span>
                <span className="font-medium">{formatCurrency(totals.reconciled)}</span>
              </div>
              <div>
                <span className="text-sm text-gray-600">Difference: </span>
                <span className={`font-medium ${Math.abs(totals.difference) < 0.01 ? 'text-green-600' : 'text-red-600'}`}>
                  {formatCurrency(totals.difference)}
                </span>
              </div>
            </div>

            <div className="flex gap-2">
              <button
                onClick={() => markReconciledMutation.mutate()}
                disabled={selectedEntries.size === 0 || markReconciledMutation.isPending}
                className="px-4 py-1.5 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2 text-sm"
              >
                {markReconciledMutation.isPending ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <CheckCircle className="w-4 h-4" />
                )}
                Post (F5)
              </button>
              <button
                onClick={() => setSelectedEntries(new Set())}
                className="px-4 py-1.5 border border-gray-400 rounded hover:bg-gray-100 text-sm"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Status Messages */}
      {markReconciledMutation.isSuccess && (
        <div className="mt-3 p-3 bg-green-50 border border-green-200 text-green-800 rounded text-sm">
          {markReconciledMutation.data?.message}
        </div>
      )}

      {markReconciledMutation.isError && (
        <div className="mt-3 p-3 bg-red-50 border border-red-200 text-red-800 rounded text-sm">
          Error: {markReconciledMutation.error?.message}
        </div>
      )}

      {/* Summary Info */}
      <div className="mt-4 text-xs text-gray-500">
        <span>{filteredEntries.length} unreconciled entries</span>
        {selectedEntries.size > 0 && viewMode === 'manual' && (
          <span className="ml-4">{selectedEntries.size} selected for reconciliation</span>
        )}
      </div>
    </div>
  );
}

export default BankStatementReconcile;
