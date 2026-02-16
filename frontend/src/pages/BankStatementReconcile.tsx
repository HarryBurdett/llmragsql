import { useState, useMemo, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useSearchParams } from 'react-router-dom';
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
  Plus,
  HelpCircle,
} from 'lucide-react';
import apiClient, { authFetch } from '../api/client';
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
  status?: 'skipped' | 'pending';  // Statement sequence status
  reason?: 'already_processed' | 'missing_statement';
  reconciled_balance?: number;
  missing_statement_balance?: number;
  message?: string;
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
  // Opera reconciliation status - reliable data from Opera
  opera_status?: {
    reconciled_balance: number | null;
    current_balance: number | null;
    last_statement_number: number | null;
    last_reconciliation_date: string | null;
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

type ViewMode = 'manual' | 'auto';

// New interfaces for enhanced auto-reconciliation
interface StatementValidationResult {
  valid: boolean;
  expected_opening?: number;
  statement_opening?: number;
  statement_closing?: number;
  difference?: number;
  opening_matches?: boolean;
  next_statement_number?: number;
  error_message?: string;
}

interface MatchedEntry {
  statement_line: number;
  statement_date: string | null;
  statement_amount: number;
  statement_reference: string;
  statement_description: string;
  entry_number: string;
  entry_date: string;
  entry_amount: number;
  entry_reference: string;
  entry_description: string;
  confidence: number;
}

interface UnmatchedStatementLine {
  statement_line: number;
  statement_date: string | null;
  statement_amount: number;
  statement_reference: string;
  statement_description: string;
  // Auto-match fields
  matched_account?: string;
  matched_name?: string;
  match_method?: string;
  suggested_type?: 'customer' | 'supplier';
}

interface UnmatchedCashbookEntry {
  entry_number: string;
  entry_date: string;
  entry_amount: number;
  entry_reference: string;
  entry_description: string;
}

interface MatchingResult {
  success: boolean;
  auto_matched: MatchedEntry[];
  suggested_matched: MatchedEntry[];
  unmatched_statement: UnmatchedStatementLine[];
  unmatched_cashbook: UnmatchedCashbookEntry[];
  summary: {
    total_statement_lines: number;
    auto_matched_count: number;
    suggested_matched_count: number;
    unmatched_statement_count: number;
    unmatched_cashbook_count: number;
  };
  error?: string;
}

interface StatementFile {
  path: string;
  filename: string;
  folder: string;
  size: number;
  size_formatted: string;
  modified: string;
  modified_formatted: string;
  // Import status
  is_imported: boolean;
  import_date?: string;
  import_bank?: string;
  transactions_imported?: number;
  // Reconciliation status
  is_reconciled: boolean;
  reconciled_date?: string;
  reconciled_count?: number;
}

interface StatementFilesResponse {
  success: boolean;
  files: StatementFile[];
  count: number;
  imported_count: number;
  reconciled_count: number;
  error?: string;
}

// Imported statements awaiting reconciliation
interface ImportedStatement {
  id: number;
  filename: string;
  bank_code: string;
  source: 'email' | 'file';
  transactions_imported: number;
  total_receipts: number;
  total_payments: number;
  import_date: string;
  imported_by: string;
  target_system: string;
  email_id?: number;
  attachment_id?: number;
  is_reconciled: boolean;
  reconciled_date?: string;
  reconciled_count: number;
  email_subject?: string;
  email_date?: string;
  email_from?: string;
}

interface ImportedStatementsResponse {
  success: boolean;
  statements: ImportedStatement[];
  count: number;
  error?: string;
}

export function BankStatementReconcile() {
  const queryClient = useQueryClient();
  const [searchParams] = useSearchParams();

  // Get bank from URL parameter if provided (e.g., from post-import redirect)
  const urlBank = searchParams.get('bank');

  const [viewMode, setViewMode] = useState<ViewMode>('manual');
  const [selectedBank, setSelectedBank] = useState<string>(urlBank || 'BC010');
  const [selectedEntries, setSelectedEntries] = useState<Set<string>>(new Set());
  const [statementNumber, setStatementNumber] = useState<string>('');
  const [statementDate, setStatementDate] = useState<string>(
    new Date().toISOString().split('T')[0]
  );
  const [statementBalance, setStatementBalance] = useState<string>('0.00');
  const [searchTerm, setSearchTerm] = useState<string>('');
  const [sortField, setSortField] = useState<'ae_entry' | 'value_pounds' | 'ae_lstdate'>('ae_lstdate');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');

  // Enhanced auto-reconciliation state - load from sessionStorage if available
  const [validationResult, setValidationResult] = useState<StatementValidationResult | null>(() => {
    try {
      const saved = sessionStorage.getItem(`validationResult_${urlBank || 'BC010'}`);
      return saved ? JSON.parse(saved) : null;
    } catch {
      return null;
    }
  });
  const [matchingResult, setMatchingResult] = useState<MatchingResult | null>(() => {
    try {
      const saved = sessionStorage.getItem(`matchingResult_${urlBank || 'BC010'}`);
      return saved ? JSON.parse(saved) : null;
    } catch {
      return null;
    }
  });
  // Store selections by ENTRY NUMBER (not index) so they survive data refreshes
  const [selectedAutoMatches, setSelectedAutoMatches] = useState<Set<string>>(new Set());
  const [selectedSuggestedMatches, setSelectedSuggestedMatches] = useState<Set<string>>(new Set());
  const [isValidating, setIsValidating] = useState(false);
  const [openingBalance, setOpeningBalance] = useState<string>('');
  const [closingBalance, setClosingBalance] = useState<string>('');
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Create entry modal state
  const [createEntryModal, setCreateEntryModal] = useState<{
    open: boolean;
    statementLine: UnmatchedStatementLine | null;
  }>({ open: false, statementLine: null });
  const [newEntryForm, setNewEntryForm] = useState({
    accountCode: '',
    accountType: 'nominal' as 'customer' | 'supplier' | 'nominal' | 'bank_transfer',
    nominalCode: '',
    reference: '',
    description: '',
    destBank: '',
  });

  const [isCreatingEntry, setIsCreatingEntry] = useState(false);
  const [showAllTransactions, setShowAllTransactions] = useState(false);

  // Bank accounts for transfers
  interface BankAccount {
    code: string;
    name: string;
  }
  const [bankAccounts, setBankAccounts] = useState<BankAccount[]>([]);

  // Nominal accounts for NL posting
  interface NominalAccount {
    code: string;
    description: string;
  }
  const [nominalAccounts, setNominalAccounts] = useState<NominalAccount[]>([]);

  // Fetch bank accounts and nominal accounts on mount
  useEffect(() => {
    authFetch('/api/cashbook/bank-accounts')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.accounts) {
          setBankAccounts(data.accounts);
        }
      })
      .catch(err => console.error('Failed to fetch bank accounts:', err));

    authFetch('/api/gocardless/nominal-accounts')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.accounts) {
          setNominalAccounts(data.accounts);
        }
      })
      .catch(err => console.error('Failed to fetch nominal accounts:', err));
  }, []);

  // Auto-match state - load last used path for the selected bank from localStorage
  const getStoredPath = (bankCode: string) => {
    const saved = localStorage.getItem(`statementPath_${bankCode}`);
    return saved || '/Users/maccb/Downloads/bank-statements/';
  };

  // Load persisted statement result from sessionStorage (survives navigation but not browser close)
  const getStoredStatementResult = (bankCode: string): ProcessStatementResponse | null => {
    try {
      const saved = sessionStorage.getItem(`statementResult_${bankCode}`);
      return saved ? JSON.parse(saved) : null;
    } catch {
      return null;
    }
  };

  // Load persisted matching result from sessionStorage
  const getStoredMatchingResult = (bankCode: string): MatchingResult | null => {
    try {
      const saved = sessionStorage.getItem(`matchingResult_${bankCode}`);
      return saved ? JSON.parse(saved) : null;
    } catch {
      return null;
    }
  };

  // Load persisted validation result from sessionStorage
  const getStoredValidationResult = (bankCode: string): StatementValidationResult | null => {
    try {
      const saved = sessionStorage.getItem(`validationResult_${bankCode}`);
      return saved ? JSON.parse(saved) : null;
    } catch {
      return null;
    }
  };

  const [statementPath, setStatementPath] = useState<string>(() => getStoredPath(urlBank || 'BC010'));
  const [statementResult, setStatementResult] = useState<ProcessStatementResponse | null>(() => getStoredStatementResult(urlBank || 'BC010'));
  const [selectedMatches, setSelectedMatches] = useState<Set<number>>(new Set());
  const [isProcessing, setIsProcessing] = useState(false);
  const [processingError, setProcessingError] = useState<string | null>(null);
  const [useManualPath, setUseManualPath] = useState(false);
  const [selectedFile, setSelectedFile] = useState<string>('');

  // Ignore transaction state
  const [ignoreConfirm, setIgnoreConfirm] = useState<{
    date: string;
    description: string;
    amount: number;
  } | null>(null);
  const [isIgnoring, setIsIgnoring] = useState(false);

  // Persist statement result to sessionStorage when it changes
  useEffect(() => {
    if (statementResult) {
      sessionStorage.setItem(`statementResult_${selectedBank}`, JSON.stringify(statementResult));
    }
  }, [statementResult, selectedBank]);

  // Persist matching result to sessionStorage when it changes
  useEffect(() => {
    if (matchingResult) {
      sessionStorage.setItem(`matchingResult_${selectedBank}`, JSON.stringify(matchingResult));
    }
  }, [matchingResult, selectedBank]);

  // Persist validation result to sessionStorage when it changes
  useEffect(() => {
    if (validationResult) {
      sessionStorage.setItem(`validationResult_${selectedBank}`, JSON.stringify(validationResult));
    }
  }, [validationResult, selectedBank]);

  // Fetch available statement files
  const statementFilesQuery = useQuery<StatementFilesResponse>({
    queryKey: ['statementFiles'],
    queryFn: async () => {
      const response = await authFetch('/api/statement-files');
      return response.json();
    },
    staleTime: 30000, // Cache for 30 seconds
  });

  // Fetch imported statements awaiting reconciliation
  const importedStatementsQuery = useQuery<ImportedStatementsResponse>({
    queryKey: ['importedStatements', selectedBank],
    queryFn: async () => {
      const response = await authFetch(`/api/statement-files/imported-for-reconciliation?bank_code=${selectedBank}`);
      return response.json();
    },
    staleTime: 30000,
  });

  // State for selecting from imported statements
  const [selectedImportedStatement, setSelectedImportedStatement] = useState<ImportedStatement | null>(null);

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
    // Load persisted data for the new bank
    setStatementResult(getStoredStatementResult(newBank));
    setMatchingResult(getStoredMatchingResult(newBank));
    setValidationResult(getStoredValidationResult(newBank));
  };

  // Clear all persisted statement data for current bank
  const clearStatementData = () => {
    sessionStorage.removeItem(`statementResult_${selectedBank}`);
    sessionStorage.removeItem(`matchingResult_${selectedBank}`);
    sessionStorage.removeItem(`validationResult_${selectedBank}`);
    setStatementResult(null);
    setMatchingResult(null);
    setValidationResult(null);
    setSelectedMatches(new Set());
    setSelectedAutoMatches(new Set());
    setSelectedSuggestedMatches(new Set());
    setProcessingError(null);
    setOpeningBalance('');
    setClosingBalance('');
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
      // Clear statement preview data after successful reconciliation
      setStatementResult(null);
      setStatementPath('');
      setProcessingError(null);
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
    setProcessingError(null);

    try {
      const response = await authFetch(
        `/api/reconcile/process-statement?file_path=${encodeURIComponent(statementPath)}&bank_code=${encodeURIComponent(selectedBank)}`,
        { method: 'POST' }
      );
      const data: ProcessStatementResponse = await response.json();

      if (data.success) {
        // Check for sequence validation status
        if (data.status === 'skipped') {
          // Earlier/already processed statement - silently ignore
          setStatementResult(null);
          setProcessingError(null);
          // Could show subtle info message if desired
          return;
        }

        if (data.status === 'pending') {
          // Future statement - missing one in between
          setStatementResult(null);
          setProcessingError(
            `Missing Statement: Cannot process this statement yet.\n\n` +
            `Statement opening balance: £${data.statement_info?.opening_balance?.toFixed(2)}\n` +
            `Opera reconciled balance: £${data.reconciled_balance?.toFixed(2)}\n\n` +
            `Please send the statement with opening balance £${data.missing_statement_balance?.toFixed(2)} to continue processing.`
          );
          return;
        }

        // Normal processing - statement is valid and in sequence
        setStatementResult(data);
        setProcessingError(null);
        // Save successful path to history for this bank
        savePathToHistory(statementPath, selectedBank);
        // Pre-select all matches
        if (data.matches) {
          setSelectedMatches(new Set(data.matches.map((_, i) => i)));
        }
        // Update statement balance and date from extracted data
        if (data.statement_info?.closing_balance != null) {
          setStatementBalance(data.statement_info.closing_balance.toString());
          setClosingBalance(data.statement_info.closing_balance.toString());
        }
        if (data.statement_info?.opening_balance != null) {
          setOpeningBalance(data.statement_info.opening_balance.toString());
        }
        // Set reconciliation date to the last transaction date on the statement
        const allTransactionDates: string[] = [];
        if (data.unmatched_statement) {
          data.unmatched_statement.forEach((t: any) => {
            if (t.date) allTransactionDates.push(t.date.split('T')[0]);
          });
        }
        if (data.matches) {
          data.matches.forEach((m: any) => {
            if (m.statement_date) allTransactionDates.push(m.statement_date.split('T')[0]);
          });
        }
        if (allTransactionDates.length > 0) {
          const lastDate = allTransactionDates.sort().pop();
          if (lastDate) setStatementDate(lastDate);
        } else if (data.statement_info?.period_end) {
          // Fallback to period_end if no transactions
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
        } else if (data.error?.includes('429') || data.error?.includes('Resource exhausted') || data.error?.includes('rate limit')) {
          setProcessingError('API Rate Limit Exceeded - The Google Gemini API has temporarily limited requests. Please wait 1-2 minutes and try again.');
        } else {
          setProcessingError(data.error || 'Unknown error occurred');
        }
      }
    } catch (error) {
      const errorMsg = String(error);
      if (errorMsg.includes('429') || errorMsg.includes('Resource exhausted') || errorMsg.includes('rate limit')) {
        setProcessingError('API Rate Limit Exceeded - The Google Gemini API has temporarily limited requests. Please wait 1-2 minutes and try again.');
      } else {
        setProcessingError(`Failed to process statement: ${error}`);
      }
    } finally {
      setIsProcessing(false);
    }
  };

  // Confirm auto-matches
  const confirmMatches = async () => {
    if (!statementResult?.matches || selectedMatches.size === 0) return;

    const matchesToConfirm = statementResult.matches.filter((_, i) => selectedMatches.has(i));

    try {
      const response = await authFetch(
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
            const archiveResponse = await authFetch(
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

  // Ignore a transaction (mark it so it won't appear in future reconciliations)
  const handleIgnoreTransaction = async () => {
    if (!ignoreConfirm) return;

    setIsIgnoring(true);
    try {
      const params = new URLSearchParams({
        transaction_date: ignoreConfirm.date,
        amount: ignoreConfirm.amount.toString(),
        description: ignoreConfirm.description,
        reason: 'Already entered in Opera'
      });

      const response = await authFetch(
        `/api/reconcile/bank/${selectedBank}/ignore-transaction?${params}`,
        { method: 'POST' }
      );
      const data = await response.json();

      if (data.success) {
        // Remove this transaction from the unmatched list (statementResult)
        if (statementResult) {
          setStatementResult({
            ...statementResult,
            unmatched_statement: statementResult.unmatched_statement?.filter(
              t => !(t.date === ignoreConfirm.date && Math.abs(t.amount - ignoreConfirm.amount) < 0.01)
            )
          });
        }
        // Also remove from matchingResult if present
        if (matchingResult) {
          setMatchingResult({
            ...matchingResult,
            unmatched_statement: matchingResult.unmatched_statement?.filter(
              t => !(t.statement_date === ignoreConfirm.date && Math.abs(t.statement_amount - ignoreConfirm.amount) < 0.01)
            ),
            summary: {
              ...matchingResult.summary,
              unmatched_statement_count: Math.max(0, matchingResult.summary.unmatched_statement_count - 1)
            }
          });
        }
        setIgnoreConfirm(null);
      } else {
        alert(`Error: ${data.error}`);
      }
    } catch (error) {
      alert(`Failed to ignore transaction: ${error}`);
    } finally {
      setIsIgnoring(false);
    }
  };

  // Validate statement opening balance against Opera's expected
  const validateStatement = async () => {
    if (!openingBalance || !closingBalance) {
      alert('Please enter both opening and closing balance');
      return;
    }

    setIsValidating(true);
    setValidationResult(null);
    setMatchingResult(null);

    try {
      const response = await authFetch(
        `/api/bank-reconciliation/validate-statement?bank_code=${selectedBank}&opening_balance=${openingBalance}&closing_balance=${closingBalance}&statement_date=${statementDate}`,
        { method: 'POST' }
      );
      const data: StatementValidationResult = await response.json();
      setValidationResult(data);

      if (data.valid && data.next_statement_number) {
        setStatementNumber(data.next_statement_number.toString());
        // Auto-run matching after successful validation
        await runMatchingFromUnreconciled();
      }

      return data;
    } catch (error) {
      setValidationResult({
        valid: false,
        error_message: `Failed to validate: ${error}`
      });
    } finally {
      setIsValidating(false);
    }
  };

  // Run matching using unreconciled entries (builds statement transactions from cashbook)
  const runMatchingFromUnreconciled = async () => {
    try {
      // Get unreconciled entries and treat them as potential statement transactions
      // For now, we'll create a simple matching based on unreconciled entries
      const entries = entriesQuery.data?.entries || [];

      // Build statement transactions from unreconciled entries for matching
      const statementTransactions = entries.map((entry, idx) => ({
        line_number: idx + 1,
        date: entry.ae_lstdate?.substring(0, 10) || '',
        amount: entry.value_pounds,
        reference: entry.ae_entref || '',
        description: entry.ae_comment || ''
      }));

      if (statementTransactions.length === 0) {
        setMatchingResult({
          success: true,
          auto_matched: [],
          suggested_matched: [],
          unmatched_statement: [],
          unmatched_cashbook: [],
          summary: {
            total_statement_lines: 0,
            auto_matched_count: 0,
            suggested_matched_count: 0,
            unmatched_statement_count: 0,
            unmatched_cashbook_count: 0
          }
        });
        return;
      }

      const response = await authFetch(
        `/api/bank-reconciliation/match-statement?bank_code=${selectedBank}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ statement_transactions: statementTransactions })
        }
      );
      const data: MatchingResult = await response.json();

      if (data.success) {
        // Auto-match unmatched statement lines to customers/suppliers
        if (data.unmatched_statement && data.unmatched_statement.length > 0) {
          data.unmatched_statement = await autoMatchUnmatchedLines(data.unmatched_statement);
        }

        setMatchingResult(data);

        // Pre-select all auto-matched entries (by entry number, not index)
        // Preserve any existing selections from previous matches
        setSelectedAutoMatches(prev => {
          const newSet = new Set(prev);
          data.auto_matched.forEach(match => newSet.add(match.entry_number));
          return newSet;
        });
        // Don't pre-select suggested matches - user should review
        // But preserve any they've already selected
      } else {
        setMatchingResult(data);
      }
    } catch (error) {
      console.error('Matching error:', error);
    }
  };

  // Complete reconciliation with selected matches
  const completeEnhancedReconciliation = async () => {
    if (!matchingResult) return;

    // Gather all selected entries (using entry_number as key)
    const selectedEntriesToReconcile: { entry_number: string; statement_line: number }[] = [];

    matchingResult.auto_matched.forEach((match) => {
      if (selectedAutoMatches.has(match.entry_number)) {
        selectedEntriesToReconcile.push({
          entry_number: match.entry_number,
          statement_line: match.statement_line
        });
      }
    });

    matchingResult.suggested_matched.forEach((match) => {
      if (selectedSuggestedMatches.has(match.entry_number)) {
        selectedEntriesToReconcile.push({
          entry_number: match.entry_number,
          statement_line: match.statement_line
        });
      }
    });

    if (selectedEntriesToReconcile.length === 0) {
      alert('No entries selected for reconciliation');
      return;
    }

    // Check for unmatched statement lines
    if (matchingResult.unmatched_statement.length > 0) {
      const proceed = window.confirm(
        `There are ${matchingResult.unmatched_statement.length} unmatched statement lines. ` +
        `Reconciliation cannot be completed until all lines are matched.\n\n` +
        `Would you like to save progress anyway?`
      );
      if (!proceed) return;
    }

    try {
      const stmtNo = parseInt(statementNumber) || (statusQuery.data?.last_stmt_no || 0) + 1;

      const response = await authFetch(
        `/api/bank-reconciliation/complete?bank_code=${selectedBank}&statement_number=${stmtNo}&statement_date=${statementDate}&closing_balance=${closingBalance}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            matched_entries: selectedEntriesToReconcile,
            statement_transactions: [] // Would need full statement transactions for gap calculation
          })
        }
      );
      const data = await response.json();

      if (data.success) {
        // Mark the statement file as reconciled in the database
        const selectedFileInfo = statementFilesQuery.data?.files?.find(f => f.path === statementPath);
        if (selectedFileInfo?.filename) {
          try {
            await authFetch(
              `/api/statement-files/mark-reconciled?filename=${encodeURIComponent(selectedFileInfo.filename)}&bank_code=${selectedBank}&reconciled_count=${data.entries_reconciled}`,
              { method: 'POST' }
            );
          } catch (e) {
            console.warn('Could not mark statement as reconciled:', e);
          }
        }

        alert(`Successfully reconciled ${data.entries_reconciled} entries!`);
        // Reset state - clear all statement preview data
        setMatchingResult(null);
        setValidationResult(null);
        setSelectedAutoMatches(new Set());
        setSelectedSuggestedMatches(new Set());
        setOpeningBalance('');
        setClosingBalance('');
        // Clear statement preview data
        setStatementResult(null);
        setStatementPath('');
        setProcessingError(null);
        // Refresh queries
        queryClient.invalidateQueries({ queryKey: ['bankRecStatus', selectedBank] });
        queryClient.invalidateQueries({ queryKey: ['unreconciledEntries', selectedBank] });
        queryClient.invalidateQueries({ queryKey: ['statementFiles'] });
      } else {
        alert(`Error: ${data.error || data.messages?.join(', ')}`);
      }
    } catch (error) {
      alert(`Failed to complete reconciliation: ${error}`);
    }
  };

  // Auto-match unmatched statement lines to customers/suppliers
  const autoMatchUnmatchedLines = async (lines: UnmatchedStatementLine[]): Promise<UnmatchedStatementLine[]> => {
    if (!lines || lines.length === 0) return lines;

    try {
      const response = await authFetch('/api/cashbook/auto-match-statement-lines', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ lines }),
      });
      const data = await response.json();

      if (data.success && data.lines) {
        return data.lines;
      }
    } catch (error) {
      console.error('Auto-match failed:', error);
    }
    return lines;
  };

  // Create cashbook entry for unmatched statement line
  const createCashbookEntry = async () => {
    if (!createEntryModal.statementLine) return;

    const line = createEntryModal.statementLine;
    setIsCreatingEntry(true);

    try {
      let data;

      // Handle bank transfer separately
      if (newEntryForm.accountType === 'bank_transfer') {
        if (!newEntryForm.destBank) {
          alert('Please select a destination bank account');
          setIsCreatingEntry(false);
          return;
        }

        // For bank transfers, source bank is the current bank account
        // Determine source/dest based on amount direction
        // Negative amount = money going OUT from this bank (this bank is source)
        // Positive amount = money coming IN to this bank (this bank is destination)
        const isOutgoing = line.statement_amount < 0;
        const sourceBank = isOutgoing ? selectedBank : newEntryForm.destBank;
        const destBank = isOutgoing ? newEntryForm.destBank : selectedBank;

        const params = new URLSearchParams({
          source_bank: sourceBank,
          dest_bank: destBank,
          amount: Math.abs(line.statement_amount).toString(),
          reference: newEntryForm.reference || line.statement_reference || '',
          date: line.statement_date || statementDate,
          comment: newEntryForm.description || line.statement_description || '',
        });

        const response = await authFetch(`/api/cashbook/create-bank-transfer?${params}`, {
          method: 'POST',
        });
        data = await response.json();

        if (data.success) {
          alert(`Bank transfer created:\n${data.source_entry} (${sourceBank}) -> ${data.dest_entry} (${destBank})\nAmount: £${data.amount?.toFixed(2)}`);
        }
      } else {
        // Existing customer/supplier/nominal logic
        let transactionType: string;
        if (line.statement_amount > 0) {
          // Money in
          if (newEntryForm.accountType === 'customer') {
            transactionType = 'sales_receipt';
          } else if (newEntryForm.accountType === 'nominal') {
            transactionType = 'nominal_receipt';
          } else {
            transactionType = 'other_receipt';
          }
        } else {
          // Money out
          if (newEntryForm.accountType === 'supplier') {
            transactionType = 'purchase_payment';
          } else if (newEntryForm.accountType === 'nominal') {
            transactionType = 'nominal_payment';
          } else {
            transactionType = 'other_payment';
          }
        }

        const requestBody = {
          bank_account: selectedBank,
          transaction_date: line.statement_date || statementDate,
          amount: Math.abs(line.statement_amount),
          reference: newEntryForm.reference || line.statement_reference,
          description: newEntryForm.description || line.statement_description,
          transaction_type: transactionType,
          account_code: newEntryForm.accountType === 'nominal' ? newEntryForm.nominalCode : newEntryForm.accountCode,
          account_type: newEntryForm.accountType,
        };

        const response = await authFetch('/api/cashbook/create-entry', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(requestBody),
        });
        data = await response.json();

        if (data.success) {
          alert(`Entry created: ${data.entry_number}`);
        }
      }

      if (data.success) {
        // Close modal and refresh
        setCreateEntryModal({ open: false, statementLine: null });
        // Re-run matching to pick up the new entry
        queryClient.invalidateQueries({ queryKey: ['unreconciledEntries', selectedBank] });
        // If we had processed a statement, re-run matching
        if (statementResult) {
          // Re-process the statement to update matches
          await processStatement();
        } else if (validationResult?.valid) {
          // Re-run matching from unreconciled
          await runMatchingFromUnreconciled();
        }
      } else {
        alert(`Error creating entry: ${data.error}`);
      }
    } catch (error) {
      alert(`Failed to create entry: ${error}`);
    } finally {
      setIsCreatingEntry(false);
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

      {/* Warning: Reconciliation in progress in Opera */}
      {statusQuery.data?.reconciliation_in_progress && (
        <div className="bg-red-50 border border-red-300 rounded-lg p-4 mb-4 flex items-start gap-3">
          <span className="text-red-500 text-xl">⚠</span>
          <div className="flex-1">
            <h3 className="font-semibold text-red-800">Reconciliation In Progress in Opera</h3>
            <p className="text-red-700 text-sm mt-1">
              {statusQuery.data.reconciliation_in_progress_message ||
                `There are ${statusQuery.data.partial_entries || 0} entries with partial reconciliation markers in Opera.`}
            </p>
            <p className="text-red-600 text-sm mt-2 font-medium">
              Please clear or complete the reconciliation in Opera before processing statements here.
            </p>
          </div>
        </div>
      )}

      {viewMode === 'auto' ? (
        /* ==================== AUTO-MATCH MODE ==================== */
        <div>
          {/* Statement Upload Section */}
          <div className={`rounded-lg p-4 mb-4 border ${statementResult ? 'bg-amber-50 border-amber-300' : 'bg-blue-50 border-blue-200'}`}>
            <div className="flex items-center gap-2 mb-3">
              <Upload className={`w-5 h-5 ${statementResult ? 'text-amber-600' : 'text-blue-600'}`} />
              <h2 className={`font-medium ${statementResult ? 'text-amber-900' : 'text-blue-900'}`}>
                {statementResult ? 'Statement In Preview' : 'Process Bank Statement'}
              </h2>
              {statementResult && (
                <span className="ml-2 px-2 py-0.5 text-xs font-semibold rounded-full bg-amber-200 text-amber-800">
                  IN PREVIEW
                </span>
              )}
              {/* Show import/reconciliation status of selected file */}
              {(() => {
                const selectedFileInfo = statementFilesQuery.data?.files?.find(f => f.path === statementPath);
                if (!selectedFileInfo) return null;

                if (selectedFileInfo.is_reconciled) {
                  return (
                    <span className="ml-2 px-2 py-0.5 text-xs font-semibold rounded-full bg-green-200 text-green-800 flex items-center gap-1">
                      <CheckCircle className="w-3 h-3" />
                      RECONCILED
                      {selectedFileInfo.reconciled_count && (
                        <span className="text-green-600">({selectedFileInfo.reconciled_count} entries)</span>
                      )}
                    </span>
                  );
                } else if (selectedFileInfo.is_imported) {
                  return (
                    <span className="ml-2 px-2 py-0.5 text-xs font-semibold rounded-full bg-amber-200 text-amber-800 flex items-center gap-1">
                      <AlertCircle className="w-3 h-3" />
                      IMPORTED - NOT RECONCILED
                      {selectedFileInfo.transactions_imported && (
                        <span className="text-amber-600">({selectedFileInfo.transactions_imported} txns)</span>
                      )}
                    </span>
                  );
                }
                return null;
              })()}
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
                    className={`text-xs ${statementResult ? 'text-amber-600 hover:text-amber-800' : 'text-blue-600 hover:text-blue-800'}`}
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
                      value={statementPath}
                      onChange={e => {
                        setStatementPath(e.target.value);
                        setSelectedFile(e.target.value);
                      }}
                      className="w-full border border-gray-300 rounded px-3 py-2 pr-8 appearance-none bg-white"
                    >
                      <option value="">-- Select a statement file --</option>
                      {statementFilesQuery.data?.files?.map(file => {
                        // Determine status icon and label
                        let statusIcon = '○';  // Not imported
                        let statusLabel = '';
                        if (file.is_reconciled) {
                          statusIcon = '✓';
                          statusLabel = ' [RECONCILED]';
                        } else if (file.is_imported) {
                          statusIcon = '⬤';
                          statusLabel = ' [IMPORTED]';
                        }
                        return (
                          <option
                            key={file.path}
                            value={file.path}
                            className={file.is_reconciled ? 'text-green-700' : file.is_imported ? 'text-amber-700' : ''}
                          >
                            {statusIcon} [{file.folder}] {file.filename} ({file.modified_formatted}){statusLabel}
                          </option>
                        );
                      })}
                    </select>
                    <ChevronDown className="w-4 h-4 absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 pointer-events-none" />
                    {statementFilesQuery.data && (
                      <p className="text-xs text-gray-500 mt-1">
                        {statementFilesQuery.data.count} files
                        {statementFilesQuery.data.imported_count > 0 && (
                          <span className="text-amber-600 ml-2">
                            ({statementFilesQuery.data.imported_count} imported)
                          </span>
                        )}
                        {statementFilesQuery.data.reconciled_count > 0 && (
                          <span className="text-green-600 ml-1">
                            ({statementFilesQuery.data.reconciled_count} reconciled)
                          </span>
                        )}
                      </p>
                    )}
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

              {/* Preview PDF Button */}
              <button
                onClick={() => {
                  if (statementPath.trim()) {
                    window.open(`http://localhost:8000/api/file/view?path=${encodeURIComponent(statementPath)}`, '_blank');
                  }
                }}
                disabled={!statementPath.trim()}
                className="px-4 py-2 bg-gray-100 text-gray-700 border border-gray-300 rounded disabled:opacity-50 flex items-center gap-2 hover:bg-gray-200"
                title="Preview the PDF statement"
              >
                <Search className="w-4 h-4" />
                Preview
              </button>

              <button
                onClick={processStatement}
                disabled={isProcessing || !statementPath.trim()}
                className={`px-4 py-2 text-white rounded disabled:opacity-50 flex items-center gap-2 ${statementResult ? 'bg-amber-600 hover:bg-amber-700' : 'bg-blue-600 hover:bg-blue-700'}`}
              >
                {isProcessing ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <FileText className="w-4 h-4" />
                )}
                {isProcessing ? 'Processing...' : statementResult ? 'Process New Statement' : 'Process Statement'}
              </button>

              {/* Clear button - only show if there's data to clear */}
              {(statementResult || matchingResult || validationResult) && (
                <button
                  onClick={clearStatementData}
                  className="px-4 py-2 bg-gray-100 text-gray-700 border border-gray-300 rounded flex items-center gap-2 hover:bg-red-50 hover:border-red-300 hover:text-red-700"
                  title="Clear all statement data and start fresh"
                >
                  <X className="w-4 h-4" />
                  Clear
                </button>
              )}
            </div>

            {/* Imported Statements Awaiting Reconciliation */}
            {importedStatementsQuery.data?.statements && importedStatementsQuery.data.statements.length > 0 && (
              <div className="mt-3 p-3 bg-green-50 border border-green-200 rounded-lg">
                <div className="flex items-start gap-2">
                  <CheckCircle className="w-5 h-5 text-green-600 mt-0.5 flex-shrink-0" />
                  <div className="flex-1">
                    <p className="text-sm text-green-800 font-medium mb-2">
                      {importedStatementsQuery.data.count} imported statement{importedStatementsQuery.data.count > 1 ? 's' : ''} ready for reconciliation
                    </p>
                    <div className="space-y-2">
                      {importedStatementsQuery.data.statements.slice(0, 3).map(stmt => (
                        <div
                          key={stmt.id}
                          className={`flex items-center justify-between p-2 rounded cursor-pointer transition-colors ${
                            selectedImportedStatement?.id === stmt.id
                              ? 'bg-green-200 border border-green-400'
                              : 'bg-white border border-green-100 hover:bg-green-100'
                          }`}
                          onClick={() => setSelectedImportedStatement(stmt)}
                        >
                          <div className="flex-1">
                            <p className="text-sm font-medium text-gray-800">{stmt.filename}</p>
                            <p className="text-xs text-gray-600">
                              {stmt.source === 'email' ? '📧 Email' : '📄 File'} •
                              {stmt.transactions_imported} txns •
                              Imported {new Date(stmt.import_date).toLocaleDateString()}
                              {stmt.email_subject && <span className="ml-1 text-gray-500">• {stmt.email_subject}</span>}
                            </p>
                          </div>
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              setSelectedImportedStatement(stmt);
                              // Trigger reconciliation for this imported statement
                              setViewMode('manual');
                            }}
                            className="px-3 py-1 text-sm bg-green-600 text-white rounded hover:bg-green-700"
                          >
                            Reconcile
                          </button>
                        </div>
                      ))}
                    </div>
                    {selectedImportedStatement && (
                      <p className="text-xs text-green-700 mt-2">
                        ✓ Selected: {selectedImportedStatement.filename} - Switch to Manual Mode to reconcile
                      </p>
                    )}
                  </div>
                </div>
              </div>
            )}

            {/* Info for imported-but-not-reconciled statements */}
            {(() => {
              const selectedFileInfo = statementFilesQuery.data?.files?.find(f => f.path === statementPath);
              if (selectedFileInfo?.is_imported && !selectedFileInfo?.is_reconciled) {
                return (
                  <div className="mt-3 p-3 bg-blue-50 border border-blue-200 rounded-lg">
                    <div className="flex items-start gap-2">
                      <AlertCircle className="w-5 h-5 text-blue-600 mt-0.5 flex-shrink-0" />
                      <div className="flex-1">
                        <p className="text-sm text-blue-800 font-medium">
                          Statement was imported on {selectedFileInfo.import_date ? new Date(selectedFileInfo.import_date).toLocaleDateString() : 'unknown date'}
                          {' '}({selectedFileInfo.transactions_imported || 0} transactions).
                        </p>
                        <p className="text-sm text-blue-700">
                          Click "Process Statement" to continue with reconciliation - imported entries will show as matched.
                        </p>
                      </div>
                    </div>
                  </div>
                );
              }
              return null;
            })()}

            {/* Processing Error Display */}
            {processingError && (
              <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
                <span className="text-red-500 mt-0.5">⚠</span>
                <div className="flex-1">
                  <p className="text-sm text-red-800 font-medium">Processing Failed</p>
                  <p className="text-sm text-red-700">{processingError}</p>
                </div>
                <button
                  onClick={() => setProcessingError(null)}
                  className="text-red-400 hover:text-red-600 text-lg leading-none"
                >
                  ×
                </button>
              </div>
            )}
          </div>

          {/* Statement Results */}
          {statementResult && (
            <div className="space-y-4">
              {/* Opera Reconciliation Status - reliable data from Opera */}
              {statementResult.opera_status && (
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                  <h3 className="font-medium mb-2 flex items-center gap-2 text-blue-800">
                    <FileText className="w-4 h-4" />
                    Last Reconciled Position (Opera)
                  </h3>
                  <div className="grid grid-cols-4 gap-4 text-sm">
                    <div>
                      <span className="text-gray-600">Last Statement:</span>{' '}
                      <span className="font-medium text-blue-900">#{statementResult.opera_status.last_statement_number || 0}</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Last Reconciled:</span>{' '}
                      <span className="font-medium text-blue-900">{formatDate(statementResult.opera_status.last_reconciliation_date)}</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Reconciled Balance:</span>{' '}
                      <span className="font-medium text-blue-900">{formatCurrency(statementResult.opera_status.reconciled_balance)}</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Current Book Balance:</span>{' '}
                      <span className="font-medium text-blue-900">{formatCurrency(statementResult.opera_status.current_balance)}</span>
                    </div>
                  </div>
                  <p className="text-xs text-blue-600 mt-2">
                    The next statement should have an opening balance of {formatCurrency(statementResult.opera_status.reconciled_balance)}
                  </p>
                </div>
              )}

              {/* Preview Statement Button */}
              <div className="flex justify-end">
                <button
                  onClick={() => setShowAllTransactions(!showAllTransactions)}
                  className="flex items-center gap-2 px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                >
                  <Search className="w-4 h-4" />
                  {showAllTransactions ? 'Hide Preview' : 'Preview Statement'} ({statementResult.extracted_transactions} transactions)
                  <ChevronDown className={`w-4 h-4 transition-transform ${showAllTransactions ? 'rotate-180' : ''}`} />
                </button>
              </div>

              {/* All Statement Transactions (Collapsible) */}
              {showAllTransactions && (
                <div className="bg-white border border-blue-200 rounded-lg overflow-hidden">
                  <div className="bg-blue-50 px-4 py-3 border-b border-blue-200">
                    <h3 className="font-medium text-blue-900 flex items-center gap-2">
                      <FileText className="w-4 h-4" />
                      All Statement Transactions
                    </h3>
                    <p className="text-xs text-blue-700 mt-1">Review all extracted transactions from the statement before proceeding</p>
                  </div>
                  <div className="max-h-96 overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          <th className="px-3 py-2 text-left">#</th>
                          <th className="px-3 py-2 text-left">Date</th>
                          <th className="px-3 py-2 text-left">Description</th>
                          <th className="px-3 py-2 text-right">Amount</th>
                          <th className="px-3 py-2 text-right">Balance</th>
                          <th className="px-3 py-2 text-center">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {/* Combine matched and unmatched transactions */}
                        {[
                          ...(statementResult.matches?.map((m, i) => ({
                            idx: i,
                            date: m.statement_txn.date,
                            description: m.statement_txn.description,
                            amount: m.statement_txn.amount,
                            balance: m.statement_txn.balance,
                            matched: true,
                          })) || []),
                          ...(statementResult.unmatched_statement?.map((t, i) => ({
                            idx: (statementResult.matches?.length || 0) + i,
                            date: t.date,
                            description: t.description,
                            amount: t.amount,
                            balance: t.balance,
                            matched: false,
                          })) || []),
                        ]
                          .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime())
                          .map((txn, idx) => (
                            <tr key={idx} className={`border-t ${txn.matched ? 'bg-green-50' : 'bg-orange-50'}`}>
                              <td className="px-3 py-2 text-gray-500">{idx + 1}</td>
                              <td className="px-3 py-2">{formatDate(txn.date)}</td>
                              <td className="px-3 py-2 max-w-xs truncate" title={txn.description}>
                                {txn.description}
                              </td>
                              <td className={`px-3 py-2 text-right font-medium ${txn.amount >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                {formatCurrency(txn.amount)}
                              </td>
                              <td className="px-3 py-2 text-right text-gray-600">
                                {txn.balance != null ? formatCurrency(txn.balance) : '-'}
                              </td>
                              <td className="px-3 py-2 text-center">
                                {txn.matched ? (
                                  <span className="inline-flex items-center gap-1 text-xs text-green-700 bg-green-100 px-2 py-0.5 rounded">
                                    <Check className="w-3 h-3" /> Matched
                                  </span>
                                ) : (
                                  <span className="inline-flex items-center gap-1 text-xs text-orange-700 bg-orange-100 px-2 py-0.5 rounded">
                                    <AlertCircle className="w-3 h-3" /> Unmatched
                                  </span>
                                )}
                              </td>
                            </tr>
                          ))}
                      </tbody>
                    </table>
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
                          <th className="px-3 py-2 text-center w-20">Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        {statementResult.unmatched_statement.map((txn, idx) => (
                          <tr key={idx} className="border-t hover:bg-gray-50">
                            <td className="px-3 py-2">{formatDate(txn.date)}</td>
                            <td className="px-3 py-2 text-gray-600 truncate max-w-md" title={txn.description}>
                              {txn.description}
                            </td>
                            <td className={`px-3 py-2 text-right ${txn.amount < 0 ? 'text-red-600' : 'text-green-600'}`}>
                              {txn.amount < 0 ? '-' : '+'}
                              {formatCurrency(txn.amount)}
                            </td>
                            <td className="px-3 py-2 text-center">
                              <button
                                onClick={() => setIgnoreConfirm({
                                  date: txn.date,
                                  description: txn.description,
                                  amount: txn.amount
                                })}
                                className="text-xs px-2 py-1 text-orange-600 hover:text-orange-800 hover:bg-orange-50 rounded"
                                title="Ignore this transaction (already in Opera)"
                              >
                                Ignore
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="px-4 py-2 bg-orange-50 border-t border-orange-200 text-xs text-orange-700">
                    💡 Use "Ignore" for transactions already entered in Opera (e.g., manual GoCardless receipts)
                  </div>
                </div>
              )}

              {/* Guidance when no matches found */}
              {(!statementResult.matches || statementResult.matches.length === 0) && statementResult.unmatched_statement && statementResult.unmatched_statement.length > 0 && (
                <div className="bg-amber-50 border border-amber-300 rounded-lg p-4">
                  <h4 className="font-medium text-amber-800 mb-2 flex items-center gap-2">
                    <AlertCircle className="w-4 h-4" />
                    No Automatic Matches Found
                  </h4>
                  <p className="text-sm text-amber-700 mb-3">
                    The statement transactions couldn't be matched to Opera entries. This usually means:
                  </p>
                  <ul className="text-sm text-amber-700 list-disc list-inside mb-3 space-y-1">
                    <li>The transactions haven't been imported yet (use Imports page first)</li>
                    <li>The entries were imported with different dates</li>
                    <li>You're processing a different statement than the one imported</li>
                  </ul>
                  <p className="text-sm text-amber-700">
                    <strong>Options:</strong> Use the "Statement Balance Validation" section below to manually select and reconcile Opera entries,
                    or switch to "Manual Mode" for direct entry selection.
                  </p>
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
                {statementResult.matches && statementResult.matches.length > 0 && (
                  <button
                    onClick={confirmMatches}
                    disabled={selectedMatches.size === 0}
                    className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
                  >
                    <Check className="w-4 h-4" />
                    Reconcile {selectedMatches.size} Matches
                  </button>
                )}
                {/* When no matches, offer to skip to manual reconciliation */}
                {(!statementResult.matches || statementResult.matches.length === 0) && (
                  <button
                    onClick={() => setViewMode('manual')}
                    className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 flex items-center gap-2"
                  >
                    <Check className="w-4 h-4" />
                    Switch to Manual Mode
                  </button>
                )}
              </div>
            </div>
          )}

          {/* Enhanced Reconciliation Section */}
          <div className="mt-6 border-t border-gray-300 pt-6">
            <div className="flex items-center gap-2 mb-4">
              <Landmark className="w-5 h-5 text-blue-600" />
              <h2 className="font-medium text-gray-900">Statement Balance Validation</h2>
              <div className="group relative">
                <HelpCircle className="w-4 h-4 text-gray-400 cursor-help" />
                <div className="hidden group-hover:block absolute left-0 top-6 w-64 p-2 bg-gray-800 text-white text-xs rounded shadow-lg z-10">
                  Enter the opening and closing balance from your bank statement.
                  The opening balance must match Opera's expected balance from the last reconciliation.
                </div>
              </div>
            </div>

            {/* Balance Inputs */}
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 mb-4">
              <div className="grid grid-cols-4 gap-4">
                <div>
                  <label className="text-sm text-gray-600 block mb-1">Opening Balance</label>
                  <div className="flex items-center">
                    <span className="text-gray-500 mr-1">£</span>
                    <input
                      type="number"
                      step="0.01"
                      value={openingBalance}
                      onChange={e => setOpeningBalance(e.target.value)}
                      placeholder="0.00"
                      className="w-full border border-gray-300 rounded px-2 py-1"
                    />
                  </div>
                  {statusQuery.data && (
                    <p className="text-xs text-gray-500 mt-1">
                      Expected: £{statusQuery.data.reconciled_balance?.toLocaleString('en-GB', { minimumFractionDigits: 2 })}
                    </p>
                  )}
                </div>
                <div>
                  <label className="text-sm text-gray-600 block mb-1">Closing Balance</label>
                  <div className="flex items-center">
                    <span className="text-gray-500 mr-1">£</span>
                    <input
                      type="number"
                      step="0.01"
                      value={closingBalance}
                      onChange={e => setClosingBalance(e.target.value)}
                      placeholder="0.00"
                      className="w-full border border-gray-300 rounded px-2 py-1"
                    />
                  </div>
                </div>
                <div>
                  <label className="text-sm text-gray-600 block mb-1">Statement Date</label>
                  <input
                    type="date"
                    value={statementDate}
                    onChange={e => setStatementDate(e.target.value)}
                    className="w-full border border-gray-300 rounded px-2 py-1"
                  />
                </div>
                <div className="flex items-end">
                  <button
                    onClick={validateStatement}
                    disabled={isValidating || !openingBalance || !closingBalance}
                    className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2"
                  >
                    {isValidating ? (
                      <RefreshCw className="w-4 h-4 animate-spin" />
                    ) : (
                      <Check className="w-4 h-4" />
                    )}
                    Validate
                  </button>
                </div>
              </div>
            </div>

            {/* Validation Result */}
            {validationResult && (
              <div className={`p-4 rounded-lg mb-4 ${
                validationResult.valid
                  ? 'bg-green-50 border border-green-200'
                  : 'bg-red-50 border border-red-200'
              }`}>
                <div className="flex items-center gap-2">
                  {validationResult.valid ? (
                    <CheckCircle className="w-5 h-5 text-green-600" />
                  ) : (
                    <AlertCircle className="w-5 h-5 text-red-600" />
                  )}
                  <span className={`font-medium ${validationResult.valid ? 'text-green-800' : 'text-red-800'}`}>
                    {validationResult.valid
                      ? 'Opening balance validated - ready to reconcile'
                      : 'Opening balance mismatch'}
                  </span>
                </div>
                {!validationResult.valid && validationResult.error_message && (
                  <p className="mt-2 text-sm text-red-700">{validationResult.error_message}</p>
                )}
                {validationResult.valid && validationResult.next_statement_number && (
                  <p className="mt-2 text-sm text-green-700">
                    Statement number: {validationResult.next_statement_number}
                  </p>
                )}
              </div>
            )}

            {/* Matching Results - Simple Statement Lines View */}
            {matchingResult && matchingResult.success && (
              <div className="space-y-4">
                {/* Statement Lines Table - clean and simple */}
                <div className="bg-white border border-gray-300 rounded-lg overflow-hidden">
                  <div className="bg-gray-100 px-4 py-2 border-b border-gray-300 flex justify-between items-center">
                    <h3 className="font-medium text-gray-800">
                      Statement Lines ({matchingResult.summary.total_statement_lines})
                      {matchingResult.summary.unmatched_statement_count > 0 && (
                        <span className="ml-2 text-red-600 text-sm">
                          ({matchingResult.summary.unmatched_statement_count} exception{matchingResult.summary.unmatched_statement_count > 1 ? 's' : ''})
                        </span>
                      )}
                    </h3>
                  </div>
                  <div className="max-h-96 overflow-y-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          <th className="px-3 py-2 text-left">Date</th>
                          <th className="px-3 py-2 text-left">Description</th>
                          <th className="px-3 py-2 text-right">Payments</th>
                          <th className="px-3 py-2 text-right">Receipts</th>
                          <th className="w-16 px-3 py-2 text-center">Line</th>
                        </tr>
                      </thead>
                      <tbody>
                        {/* Combine all lines and sort by statement_line */}
                        {(() => {
                          // Build unified list of all statement lines
                          const allLines: Array<{
                            statement_line: number;
                            statement_date: string | null;
                            statement_reference: string;
                            statement_description: string;
                            statement_amount: number;
                            entry_number: string | null;
                            type: 'matched' | 'unmatched';
                          }> = [
                            ...matchingResult.auto_matched.map(m => ({
                              statement_line: m.statement_line,
                              statement_date: m.statement_date,
                              statement_reference: m.statement_reference,
                              statement_description: m.statement_description,
                              statement_amount: m.statement_amount,
                              entry_number: m.entry_number,
                              type: 'matched' as const
                            })),
                            ...matchingResult.suggested_matched.map(m => ({
                              statement_line: m.statement_line,
                              statement_date: m.statement_date,
                              statement_reference: m.statement_reference,
                              statement_description: m.statement_description,
                              statement_amount: m.statement_amount,
                              entry_number: m.entry_number,
                              type: 'matched' as const
                            })),
                            ...matchingResult.unmatched_statement.map(u => ({
                              statement_line: u.statement_line,
                              statement_date: u.statement_date,
                              statement_reference: u.statement_reference,
                              statement_description: u.statement_description,
                              statement_amount: u.statement_amount,
                              entry_number: null,
                              type: 'unmatched' as const
                            }))
                          ].sort((a, b) => a.statement_line - b.statement_line);

                          // Auto-select all matched entries on first render
                          if (selectedAutoMatches.size === 0 && selectedSuggestedMatches.size === 0) {
                            setTimeout(() => {
                              setSelectedAutoMatches(new Set(matchingResult.auto_matched.map(m => m.entry_number)));
                              setSelectedSuggestedMatches(new Set(matchingResult.suggested_matched.map(m => m.entry_number)));
                            }, 0);
                          }

                          return allLines.map((line) => {
                            const isException = line.type === 'unmatched';

                            return (
                              <tr
                                key={line.statement_line}
                                className={`border-t ${isException ? 'bg-red-50' : ''}`}
                              >
                                <td className="px-3 py-2 text-gray-600">{formatDate(line.statement_date)}</td>
                                <td className="px-3 py-2">
                                  <div className="truncate max-w-md">{line.statement_reference || line.statement_description}</div>
                                  {isException && (
                                    <span className="text-xs text-red-600">⚠ No matching entry</span>
                                  )}
                                </td>
                                <td className="px-3 py-2 text-right font-medium text-red-600">
                                  {line.statement_amount < 0 ? formatCurrency(Math.abs(line.statement_amount)) : ''}
                                </td>
                                <td className="px-3 py-2 text-right font-medium text-green-600">
                                  {line.statement_amount >= 0 ? formatCurrency(line.statement_amount) : ''}
                                </td>
                                <td className="px-3 py-2 text-center font-medium text-gray-700">{line.statement_line * 10}</td>
                              </tr>
                            );
                          });
                        })()}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Complete Reconciliation Button */}
                <div className="flex justify-end gap-3 pt-4">
                  <button
                    onClick={() => {
                      setMatchingResult(null);
                      setSelectedAutoMatches(new Set());
                      setSelectedSuggestedMatches(new Set());
                    }}
                    className="px-4 py-2 border border-gray-300 rounded hover:bg-gray-50 flex items-center gap-2"
                  >
                    <X className="w-4 h-4" />
                    Cancel
                  </button>
                  <button
                    onClick={async () => {
                      setIsRefreshing(true);
                      try {
                        // Re-run matching but preserve existing selections
                        await runMatchingFromUnreconciled();
                      } finally {
                        setIsRefreshing(false);
                      }
                    }}
                    disabled={isRefreshing}
                    className="px-4 py-2 border border-blue-300 text-blue-700 rounded hover:bg-blue-50 disabled:opacity-50 flex items-center gap-2"
                    title="Refresh cashbook data (preserves your selections)"
                  >
                    <RefreshCw className={`w-4 h-4 ${isRefreshing ? 'animate-spin' : ''}`} />
                    Refresh
                  </button>
                  <button
                    onClick={completeEnhancedReconciliation}
                    disabled={
                      (matchingResult?.auto_matched.filter(m => selectedAutoMatches.has(m.entry_number)).length || 0) +
                      (matchingResult?.suggested_matched.filter(m => selectedSuggestedMatches.has(m.entry_number)).length || 0) === 0
                    }
                    className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"
                  >
                    <Check className="w-4 h-4" />
                    Reconcile {
                      (matchingResult?.auto_matched.filter(m => selectedAutoMatches.has(m.entry_number)).length || 0) +
                      (matchingResult?.suggested_matched.filter(m => selectedSuggestedMatches.has(m.entry_number)).length || 0)
                    } Entries
                  </button>
                </div>
              </div>
            )}
          </div>

          {/* No results message */}
          {!statementResult && !isProcessing && !matchingResult && (
            <div className="bg-white border border-gray-200 rounded-lg p-8 text-center text-gray-500">
              <FileText className="w-12 h-12 mx-auto mb-3 text-gray-300" />
              <p>Process a bank statement above, or enter opening/closing balance to validate and run matching</p>
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

      {/* Create Entry Modal */}
      {createEntryModal.open && createEntryModal.statementLine && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-full max-w-md">
            <div className="px-4 py-3 border-b border-gray-200 flex justify-between items-center">
              <h3 className="font-medium text-gray-900">Create Cashbook Entry</h3>
              <button
                onClick={() => setCreateEntryModal({ open: false, statementLine: null })}
                className="text-gray-400 hover:text-gray-600"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            <div className="p-4 space-y-4">
              {/* Statement Line Info */}
              <div className="bg-gray-50 rounded p-3 text-sm">
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <span className="text-gray-500">Date:</span>{' '}
                    <span className="font-medium">{formatDate(createEntryModal.statementLine.statement_date)}</span>
                  </div>
                  <div>
                    <span className="text-gray-500">Amount:</span>{' '}
                    <span className={`font-medium ${createEntryModal.statementLine.statement_amount < 0 ? 'text-red-600' : 'text-green-600'}`}>
                      {createEntryModal.statementLine.statement_amount < 0 ? '-' : '+'}
                      £{formatCurrency(createEntryModal.statementLine.statement_amount)}
                    </span>
                  </div>
                </div>
                <div className="mt-1">
                  <span className="text-gray-500">Reference:</span>{' '}
                  <span className="font-medium font-mono text-xs">{createEntryModal.statementLine.statement_reference || '-'}</span>
                </div>
                {createEntryModal.statementLine.statement_description && (
                  <div className="mt-1">
                    <span className="text-gray-500">Description:</span>{' '}
                    <span className="text-gray-700">{createEntryModal.statementLine.statement_description}</span>
                  </div>
                )}
              </div>

              {/* Account Type */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Entry Type</label>
                <div className="grid grid-cols-2 gap-2">
                  <button
                    type="button"
                    onClick={() => setNewEntryForm({ ...newEntryForm, accountType: 'customer', accountCode: '', nominalCode: '', destBank: '' })}
                    className={`px-3 py-2 rounded border text-sm ${
                      newEntryForm.accountType === 'customer'
                        ? 'bg-blue-100 border-blue-500 text-blue-700'
                        : 'border-gray-300 hover:bg-gray-50'
                    }`}
                  >
                    Customer
                  </button>
                  <button
                    type="button"
                    onClick={() => setNewEntryForm({ ...newEntryForm, accountType: 'supplier', accountCode: '', nominalCode: '', destBank: '' })}
                    className={`px-3 py-2 rounded border text-sm ${
                      newEntryForm.accountType === 'supplier'
                        ? 'bg-blue-100 border-blue-500 text-blue-700'
                        : 'border-gray-300 hover:bg-gray-50'
                    }`}
                  >
                    Supplier
                  </button>
                  <button
                    type="button"
                    onClick={() => setNewEntryForm({ ...newEntryForm, accountType: 'bank_transfer', accountCode: '', nominalCode: '', destBank: '' })}
                    className={`px-3 py-2 rounded border text-sm ${
                      newEntryForm.accountType === 'bank_transfer'
                        ? 'bg-purple-100 border-purple-500 text-purple-700'
                        : 'border-gray-300 hover:bg-gray-50'
                    }`}
                  >
                    Bank Transfer
                  </button>
                  <button
                    type="button"
                    onClick={() => setNewEntryForm({ ...newEntryForm, accountType: 'nominal', accountCode: '', destBank: '' })}
                    className={`px-3 py-2 rounded border text-sm ${
                      newEntryForm.accountType === 'nominal'
                        ? 'bg-blue-100 border-blue-500 text-blue-700'
                        : 'border-gray-300 hover:bg-gray-50'
                    }`}
                  >
                    Nominal
                  </button>
                </div>
              </div>

              {/* Bank Transfer - Destination Bank */}
              {newEntryForm.accountType === 'bank_transfer' && (
                <div className="space-y-3">
                  <div className="bg-purple-50 border border-purple-200 rounded p-3">
                    <div className="flex items-start gap-2">
                      <Landmark className="w-4 h-4 text-purple-600 mt-0.5" />
                      <div className="text-sm text-purple-800">
                        <strong>Bank Transfer</strong>: Creates paired entries in both bank accounts.
                        {createEntryModal.statementLine && createEntryModal.statementLine.statement_amount < 0 ? (
                          <span> Money going <strong>OUT</strong> from {selectedBank}.</span>
                        ) : (
                          <span> Money coming <strong>IN</strong> to {selectedBank}.</span>
                        )}
                      </div>
                    </div>
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      {createEntryModal.statementLine && createEntryModal.statementLine.statement_amount < 0
                        ? 'Destination Bank (receiving)'
                        : 'Source Bank (sending)'}
                    </label>
                    <select
                      value={newEntryForm.destBank}
                      onChange={e => setNewEntryForm({ ...newEntryForm, destBank: e.target.value })}
                      className="w-full border border-gray-300 rounded px-3 py-2"
                    >
                      <option value="">Select bank account...</option>
                      {bankAccounts
                        .filter(b => b.code !== selectedBank)
                        .map(b => (
                          <option key={b.code} value={b.code}>
                            {b.code} - {b.name}
                          </option>
                        ))}
                    </select>
                  </div>
                </div>
              )}

              {/* Account Code - Customer/Supplier */}
              {(newEntryForm.accountType === 'customer' || newEntryForm.accountType === 'supplier') && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    {newEntryForm.accountType === 'customer' ? 'Customer' : 'Supplier'} Code
                  </label>
                  <input
                    type="text"
                    value={newEntryForm.accountCode}
                    onChange={e => setNewEntryForm({ ...newEntryForm, accountCode: e.target.value.toUpperCase() })}
                    placeholder={newEntryForm.accountType === 'customer' ? 'e.g. A001' : 'e.g. SUP001'}
                    className="w-full border border-gray-300 rounded px-3 py-2"
                  />
                </div>
              )}

              {/* Nominal Account Selection */}
              {newEntryForm.accountType === 'nominal' && (
                <div className="space-y-3">
                  <div className="bg-blue-50 border border-blue-200 rounded p-3">
                    <div className="flex items-start gap-2">
                      <HelpCircle className="w-4 h-4 text-blue-600 mt-0.5" />
                      <div className="text-sm text-blue-800">
                        <strong>NL Posting</strong>: Posts directly to a nominal account without going through customer/supplier ledger.
                        {createEntryModal.statementLine && createEntryModal.statementLine.statement_amount < 0 ? (
                          <span> Money going <strong>OUT</strong> (e.g., bank charges, fees).</span>
                        ) : (
                          <span> Money coming <strong>IN</strong> (e.g., interest received).</span>
                        )}
                      </div>
                    </div>
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Nominal Account</label>
                    <select
                      value={newEntryForm.nominalCode}
                      onChange={e => setNewEntryForm({ ...newEntryForm, nominalCode: e.target.value })}
                      className="w-full border border-gray-300 rounded px-3 py-2"
                    >
                      <option value="">Select nominal account...</option>
                      {nominalAccounts.map(acc => (
                        <option key={acc.code} value={acc.code}>
                          {acc.code} - {acc.description}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
              )}

              {/* Reference */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Reference</label>
                <input
                  type="text"
                  value={newEntryForm.reference}
                  onChange={e => setNewEntryForm({ ...newEntryForm, reference: e.target.value })}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                />
              </div>

              {/* Description */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
                <input
                  type="text"
                  value={newEntryForm.description}
                  onChange={e => setNewEntryForm({ ...newEntryForm, description: e.target.value })}
                  className="w-full border border-gray-300 rounded px-3 py-2"
                />
              </div>
            </div>

            <div className="px-4 py-3 border-t border-gray-200 flex justify-end gap-2">
              <button
                onClick={() => setCreateEntryModal({ open: false, statementLine: null })}
                className="px-4 py-2 border border-gray-300 rounded hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={createCashbookEntry}
                disabled={
                  isCreatingEntry ||
                  (newEntryForm.accountType === 'customer' && !newEntryForm.accountCode) ||
                  (newEntryForm.accountType === 'supplier' && !newEntryForm.accountCode) ||
                  (newEntryForm.accountType === 'nominal' && !newEntryForm.nominalCode) ||
                  (newEntryForm.accountType === 'bank_transfer' && !newEntryForm.destBank)
                }
                className={`px-4 py-2 text-white rounded disabled:opacity-50 flex items-center gap-2 ${
                  newEntryForm.accountType === 'bank_transfer'
                    ? 'bg-purple-600 hover:bg-purple-700'
                    : 'bg-blue-600 hover:bg-blue-700'
                }`}
              >
                {isCreatingEntry ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : newEntryForm.accountType === 'bank_transfer' ? (
                  <Landmark className="w-4 h-4" />
                ) : (
                  <Plus className="w-4 h-4" />
                )}
                {newEntryForm.accountType === 'bank_transfer' ? 'Create Transfer' : 'Create Entry'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Ignore Transaction Confirmation Modal */}
      {ignoreConfirm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl max-w-md w-full mx-4">
            <div className="px-6 py-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900">Ignore Transaction?</h3>
            </div>
            <div className="p-6">
              <p className="text-gray-600 mb-4">
                This will permanently ignore this transaction for future bank reconciliations.
                Use this for transactions already entered in Opera (e.g., manual GoCardless receipts).
              </p>
              <div className="bg-gray-50 rounded-lg p-4 mb-4">
                <div className="grid grid-cols-2 gap-2 text-sm">
                  <div className="text-gray-500">Date:</div>
                  <div className="font-medium">{formatDate(ignoreConfirm.date)}</div>
                  <div className="text-gray-500">Amount:</div>
                  <div className={`font-medium ${ignoreConfirm.amount < 0 ? 'text-red-600' : 'text-green-600'}`}>
                    {ignoreConfirm.amount < 0 ? '-' : '+'}£{formatCurrency(ignoreConfirm.amount)}
                  </div>
                  <div className="text-gray-500">Description:</div>
                  <div className="font-medium text-xs">{ignoreConfirm.description}</div>
                </div>
              </div>
              <p className="text-sm text-orange-600 mb-4">
                ⚠️ This action cannot be undone from this screen.
              </p>
            </div>
            <div className="px-6 py-4 bg-gray-50 flex justify-end gap-3 rounded-b-lg">
              <button
                onClick={() => setIgnoreConfirm(null)}
                disabled={isIgnoring}
                className="px-4 py-2 border border-gray-300 rounded hover:bg-gray-100"
              >
                Cancel
              </button>
              <button
                onClick={handleIgnoreTransaction}
                disabled={isIgnoring}
                className="px-4 py-2 bg-orange-600 text-white rounded hover:bg-orange-700 disabled:opacity-50 flex items-center gap-2"
              >
                {isIgnoring ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <X className="w-4 h-4" />
                )}
                Yes, Ignore Transaction
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default BankStatementReconcile;
