import React, { useState, useEffect, useCallback, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { FileText, CheckCircle, XCircle, AlertCircle, Loader2, Receipt, CreditCard, FileSpreadsheet, BookOpen, Landmark, /* Upload - kept for CSV upload if re-enabled */ Edit3, RefreshCw, Search, RotateCcw, X, History, ChevronDown, ChevronRight } from 'lucide-react';
import apiClient, { authFetch } from '../api/client';

interface ImportResult {
  success: boolean;
  validate_only: boolean;
  records_processed: number;
  records_imported: number;
  records_failed: number;
  errors: string[];
  details: string[];
}

interface BankAccount {
  code: string;
  description: string;
  sort_code: string;
  account_number: string;
}

interface OperaAccount {
  code: string;
  name: string;
  display: string;
}

interface DuplicateCandidate {
  table: string;
  record_id: string;
  match_type: string;
  confidence: number;
}

type TransactionType = 'sales_receipt' | 'purchase_payment' | 'sales_refund' | 'purchase_refund' | 'nominal_receipt' | 'nominal_payment' | 'bank_transfer' | 'ignore';

// Nominal posting detail for VAT entry
interface NominalPostingDetail {
  nominalCode: string;
  nominalDescription?: string;
  vatCode: string;
  vatRate: number;
  netAmount: number;
  vatAmount: number;
  grossAmount: number;
}

// VAT code from Opera
interface VatCode {
  code: string;
  description: string;
  rate: number;
}

interface BankImportTransaction {
  row: number;
  date: string;
  type?: string;
  amount: number;
  name: string;
  reference?: string;
  memo?: string;
  fit_id?: string;
  account?: string;
  account_name?: string;
  match_score?: number;
  match_source?: string;
  action?: string;
  reason?: string;
  fingerprint?: string;
  is_duplicate?: boolean;
  duplicate_candidates?: DuplicateCandidate[];
  transaction_type?: TransactionType;
  refund_credit_note?: string;
  refund_credit_amount?: number;
  // Repeat entry fields
  repeat_entry_ref?: string;
  repeat_entry_desc?: string;
  repeat_entry_next_date?: string;
  repeat_entry_posted?: number;  // Times posted
  repeat_entry_total?: number;   // Times to post (0=unlimited)
  // For editable preview
  manual_account?: string;
  manual_ledger_type?: 'C' | 'S';
  isEdited?: boolean;
  // Period validation
  period_valid?: boolean;
  period_error?: string;
  original_date?: string;
  // Date override (user modified date)
  date_override?: string;
  // Nominal posting detail (for nominal_receipt/nominal_payment)
  nominal_detail?: NominalPostingDetail;
}

interface PeriodViolation {
  row: number;
  date: string;
  name?: string;
  amount?: number;
  action?: string;
  ledger_type?: string;
  ledger_name?: string;
  error: string;
  year?: number;
  period?: number;
  transaction_year?: number;
  transaction_period?: number;
  current_year?: number;
  current_period?: number;
}

interface StatementBankInfo {
  bank_name?: string;
  account_number?: string;
  sort_code?: string;
  statement_date?: string;
  opening_balance?: number;
  closing_balance?: number;
  matched_opera_bank?: string;
  matched_opera_name?: string;
  bank_mismatch?: boolean;
}

interface EnhancedBankImportPreview {
  success: boolean;
  filename: string;
  detected_format?: string;
  total_transactions: number;
  matched_receipts: BankImportTransaction[];
  matched_payments: BankImportTransaction[];
  matched_refunds: BankImportTransaction[];
  repeat_entries: BankImportTransaction[];
  unmatched: BankImportTransaction[];
  already_posted: BankImportTransaction[];
  skipped: BankImportTransaction[];
  summary?: {
    to_import: number;
    refund_count: number;
    repeat_entry_count: number;
    unmatched_count: number;
    already_posted_count: number;
    skipped_count: number;
  };
  errors: string[];
  // Period validation
  period_info?: {
    current_year: number;
    current_period: number;
    open_period_accounting: boolean;
  };
  period_violations?: PeriodViolation[];
  has_period_violations?: boolean;
  // Statement metadata (from AI extraction)
  statement_bank_info?: StatementBankInfo;
}

type PreviewTab = 'receipts' | 'payments' | 'refunds' | 'repeat' | 'unmatched' | 'skipped';

const API_BASE = 'http://localhost:8000/api';

type ImportType = 'bank-statement' | 'sales-receipt' | 'purchase-payment' | 'sales-invoice' | 'purchase-invoice' | 'nominal-journal';

type DataSource = 'opera-sql' | 'opera3';

export function Imports({ bankRecOnly = false }: { bankRecOnly?: boolean } = {}) {
  const [activeType, setActiveType] = useState<ImportType>('bank-statement');
  const [loading, setLoading] = useState(false);
  const [isPreviewing, setIsPreviewing] = useState(false);
  const [isImporting, setIsImporting] = useState(false);
  const [result, setResult] = useState<ImportResult | null>(null);

  // Raw file preview state
  const [rawFilePreview, setRawFilePreview] = useState<string[] | null>(null);
  const [showRawPreview, setShowRawPreview] = useState(false);
  const [validateOnly, setValidateOnly] = useState(true);

  // PDF viewer popup state (supports base64 data or direct URL)
  const [pdfViewerData, setPdfViewerData] = useState<{ data: string; filename: string; viewUrl?: string } | null>(null);

  // Data source derived from Opera settings configuration
  const { data: operaConfigData } = useQuery({
    queryKey: ['operaConfig'],
    queryFn: async () => {
      const res = await authFetch(`${API_BASE}/config/opera`);
      return res.json();
    },
  });
  const dataSource: DataSource = operaConfigData?.version === 'opera3' ? 'opera3' : 'opera-sql';

  // Get current company for company-specific localStorage keys
  const { data: companiesData } = useQuery({
    queryKey: ['companies'],
    queryFn: async () => {
      const response = await apiClient.getCompanies();
      return response.data;
    },
  });
  const currentCompanyId = companiesData?.current_company?.id || '';

  // Bank statement import state
  const [bankAccounts, setBankAccounts] = useState<BankAccount[]>([]);
  // Don't initialize from localStorage - wait for company to load
  const [selectedBankCode, setSelectedBankCode] = useState<string>('');
  const [csvDirectory, setCsvDirectory] = useState(() =>
    localStorage.getItem('bankImport_csvDirectory') || ''
  );
  const [csvFileName, setCsvFileName] = useState('');
  const [opera3DataPath, setOpera3DataPath] = useState(() =>
    localStorage.getItem('bankImport_opera3DataPath') || ''
  );

  // Auto-populate Opera 3 data path from settings if not already set
  useEffect(() => {
    if (operaConfigData && !opera3DataPath) {
      const serverPath = operaConfigData.opera3_server_path;
      const basePath = operaConfigData.opera3_base_path;
      if (serverPath) {
        setOpera3DataPath(serverPath);
      } else if (basePath) {
        setOpera3DataPath(basePath);
      }
    }
  }, [operaConfigData, opera3DataPath]);

  // Helper to convert Map to array for JSON serialization
  const mapToArray = <K, V>(map: Map<K, V>): [K, V][] => Array.from(map.entries());

  const [bankPreview, setBankPreview] = useState<EnhancedBankImportPreview | null>(null);
  const [bankImportResult, setBankImportResult] = useState<any>(null);

  // New state for editable preview
  const [editedTransactions, setEditedTransactions] = useState<Map<number, BankImportTransaction>>(new Map());

  // Tabbed preview state
  const [activePreviewTab, setActivePreviewTab] = useState<PreviewTab>('receipts');
  const [tabSearchFilter, setTabSearchFilter] = useState('');

  // Skipped items inclusion state
  const [includedSkipped, setIncludedSkipped] = useState<Map<number, {
    account: string;
    ledger_type: 'C' | 'S';
    transaction_type: TransactionType;
  }>>(new Map());

  // Transaction type overrides for unmatched items
  const [transactionTypeOverrides, setTransactionTypeOverrides] = useState<Map<number, TransactionType>>(new Map());

  // Refund overrides (for changing type/account on auto-detected refunds)
  const [refundOverrides, setRefundOverrides] = useState<Map<number, {
    transaction_type?: TransactionType;
    account?: string;
    ledger_type?: 'C' | 'S';
    rejected?: boolean;
  }>>(new Map());

  // Auto-allocate option - when enabled, receipts/payments are auto-allocated to invoices
  const [autoAllocate, setAutoAllocate] = useState(() =>
    localStorage.getItem('bankImport_autoAllocate') === 'true'
  );

  // Persist auto-allocate preference
  useEffect(() => {
    localStorage.setItem('bankImport_autoAllocate', autoAllocate ? 'true' : 'false');
  }, [autoAllocate]);

  // Show reconcile prompt after successful import
  const [showReconcilePrompt, setShowReconcilePrompt] = useState(false);
  const [reconcileSelectedEntries, setReconcileSelectedEntries] = useState<Set<string>>(new Set());
  const [unreconciledEntries, setUnreconciledEntries] = useState<any[]>([]);
  const [loadingUnreconciled, setLoadingUnreconciled] = useState(false);

  // Selection state for import - tracks which rows are selected for import across ALL tabs
  const [selectedForImport, setSelectedForImport] = useState<Set<number>>(new Set());

  // Date overrides for period violations - maps row number to new date
  const [dateOverrides, setDateOverrides] = useState<Map<number, string>>(new Map());

  // Per-row auto-allocate overrides - defaults to true (follow global setting), can be disabled per row
  // Only tracks rows where user explicitly disabled auto-allocate for that specific transaction
  const [autoAllocateDisabled, setAutoAllocateDisabled] = useState<Set<number>>(new Set());

  // Track repeat entries that have had their dates updated (ready for Opera processing)
  const [updatedRepeatEntries, setUpdatedRepeatEntries] = useState<Set<string>>(new Set());
  const [updatingRepeatEntry, setUpdatingRepeatEntry] = useState<string | null>(null);

  // Ignore transaction confirmation state
  const [ignoreConfirm, setIgnoreConfirm] = useState<{
    row: number;
    date: string;
    description: string;
    amount: number;
  } | null>(null);
  const [isIgnoring, setIsIgnoring] = useState(false);
  // Track which transactions are marked as ignored (by row number)
  const [ignoredTransactions, setIgnoredTransactions] = useState<Set<number>>(new Set());

  // =====================
  // EMAIL SCANNING STATE
  // =====================
  type StatementSource = 'file' | 'email' | 'pdf';
  const [statementSource, setStatementSource] = useState<StatementSource>('email');
  const [emailScanLoading, setEmailScanLoading] = useState(false);
  const [emailScanDaysBack, setEmailScanDaysBack] = useState(30);
  const [emailStatements, setEmailStatements] = useState<Array<{
    email_id: number;
    message_id: string;
    subject: string;
    from_address: string;
    from_name?: string;
    received_at: string;
    attachments: Array<{
      attachment_id: string;
      filename: string;
      size_bytes: number;
      content_type?: string;
      already_processed: boolean;
      statement_date?: string;
    }>;
    detected_bank: string | null;
    already_processed: boolean;
    import_sequence?: number;
    statement_date?: string;
  }>>([]);
  const [selectedEmailStatement, setSelectedEmailStatement] = useState<{
    emailId: number;
    attachmentId: string;
    filename: string;
  } | null>(null);

  // =====================
  // IMPORT HISTORY STATE
  // =====================
  const [showImportHistory, setShowImportHistory] = useState(false);
  const [importHistoryData, setImportHistoryData] = useState<Array<{
    id: number;
    filename: string;
    source: 'email' | 'file';
    bank_code: string;
    total_receipts: number;
    total_payments: number;
    transactions_imported: number;
    target_system: string;
    import_date: string;
    imported_by: string;
    email_subject?: string;
    email_from?: string;
  }>>([]);
  const [importHistoryLoading, setImportHistoryLoading] = useState(false);
  const [historyLimit, setHistoryLimit] = useState(50);
  const [historyFromDate, setHistoryFromDate] = useState('');
  const [historyToDate, setHistoryToDate] = useState('');
  const [isClearing, setIsClearing] = useState(false);
  const [showClearConfirm, setShowClearConfirm] = useState(false);
  const [showClearStatementConfirm, setShowClearStatementConfirm] = useState(false);
  const [reImportRecord, setReImportRecord] = useState<{ id: number; filename: string; amount: number } | null>(null);
  const [isDeleting, setIsDeleting] = useState(false);
  const [expandedHistoryId, setExpandedHistoryId] = useState<number | null>(null);

  // =====================
  // PDF UPLOAD STATE
  // =====================
  const [pdfDirectory, setPdfDirectory] = useState(() =>
    localStorage.getItem('bankImport_pdfDirectory') || ''
  );
  const [_pdfFileName, _setPdfFileName] = useState(''); // Reserved for future use
  const [pdfFilesList, setPdfFilesList] = useState<Array<{
    filename: string;
    modified: string;
    size_display: string;
    already_processed: boolean;
    statement_date?: string;
    import_sequence?: number;
  }> | null>(null);
  const [pdfFilesLoading, setPdfFilesLoading] = useState(false);
  const [selectedPdfFile, setSelectedPdfFile] = useState<{
    filename: string;
    fullPath: string;
  } | null>(null);

  // =====================
  // SESSION STORAGE PERSISTENCE - Keep data when switching tabs/pages
  // =====================
  const STORAGE_KEY = 'bankImportState';
  const hasRestoredFromSession = useRef(false);
  const sessionRestoreComplete = useRef(false);

  // Load persisted state on mount
  useEffect(() => {
    try {
      const saved = sessionStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved);
        // Mark that we're restoring from session - prevents other effects from clearing
        hasRestoredFromSession.current = true;

        if (parsed.bankPreview) setBankPreview(parsed.bankPreview);
        if (parsed.editedTransactions) setEditedTransactions(new Map(parsed.editedTransactions)); // Restore unmatched edits
        if (parsed.selectedForImport) setSelectedForImport(new Set(parsed.selectedForImport));
        if (parsed.dateOverrides) setDateOverrides(new Map(parsed.dateOverrides));
        if (parsed.transactionTypeOverrides) setTransactionTypeOverrides(new Map(parsed.transactionTypeOverrides));
        if (parsed.includedSkipped) setIncludedSkipped(new Map(parsed.includedSkipped));
        if (parsed.refundOverrides) setRefundOverrides(new Map(parsed.refundOverrides));
        if (parsed.nominalPostingDetails) setNominalPostingDetails(new Map(parsed.nominalPostingDetails)); // Restore nominal details
        if (parsed.bankTransferDetails) setBankTransferDetails(new Map(parsed.bankTransferDetails)); // Restore bank transfer details
        if (parsed.autoAllocateDisabled) setAutoAllocateDisabled(new Set(parsed.autoAllocateDisabled)); // Restore per-row auto-allocate disabled flags
        if (parsed.activePreviewTab) setActivePreviewTab(parsed.activePreviewTab);
        if (parsed.csvFileName) setCsvFileName(parsed.csvFileName);
        if (parsed.csvDirectory) setCsvDirectory(parsed.csvDirectory);
        if (parsed.selectedBankCode) setSelectedBankCode(parsed.selectedBankCode);
        // Restore source selections for import
        if (parsed.statementSource) setStatementSource(parsed.statementSource);
        if (parsed.selectedEmailStatement) setSelectedEmailStatement(parsed.selectedEmailStatement);
        if (parsed.selectedPdfFile) setSelectedPdfFile(parsed.selectedPdfFile);

        console.log('Restored bank import state from session:', {
          hasPreview: !!parsed.bankPreview,
          editedCount: parsed.editedTransactions?.length || 0,
          selectedCount: parsed.selectedForImport?.length || 0
        });
      }
    } catch (e) {
      console.warn('Failed to load bank import state from session storage:', e);
    }
    // Mark restore as complete after a small delay to let state updates settle
    setTimeout(() => {
      sessionRestoreComplete.current = true;
    }, 100);
  }, []);

  // Clear persisted state after successful import
  const clearPersistedState = useCallback(() => {
    sessionStorage.removeItem(STORAGE_KEY);
  }, []);

  // Fetch customers and suppliers using react-query (auto-refreshes on company switch)
  const { data: customersData } = useQuery({
    queryKey: ['bank-import-customers'],
    queryFn: async () => {
      const res = await authFetch(`${API_BASE}/bank-import/accounts/customers`);
      return res.json();
    },
  });

  const { data: suppliersData } = useQuery({
    queryKey: ['bank-import-suppliers'],
    queryFn: async () => {
      const res = await authFetch(`${API_BASE}/bank-import/accounts/suppliers`);
      return res.json();
    },
  });

  const customers: OperaAccount[] = customersData?.success ? customersData.accounts : [];
  const suppliers: OperaAccount[] = suppliersData?.success ? suppliersData.accounts : [];

  // Fetch nominal accounts for NL posting
  const { data: nominalAccountsData } = useQuery({
    queryKey: ['bank-import-nominals'],
    queryFn: async () => {
      const res = await authFetch(`${API_BASE}/gocardless/nominal-accounts`);
      return res.json();
    },
  });

  interface NominalAccount {
    code: string;
    description: string;
  }
  const nominalAccounts: NominalAccount[] = nominalAccountsData?.success ? nominalAccountsData.accounts : [];

  // Fetch VAT codes for nominal postings
  const { data: vatCodesData } = useQuery({
    queryKey: ['bank-import-vat-codes'],
    queryFn: async () => {
      const res = await authFetch(`${API_BASE}/gocardless/vat-codes`);
      return res.json();
    },
  });
  const vatCodes: VatCode[] = vatCodesData?.success ? vatCodesData.codes : [];

  // Nominal detail modal state
  const [nominalDetailModal, setNominalDetailModal] = useState<{
    open: boolean;
    transaction: BankImportTransaction | null;
    transactionType: TransactionType | null;
    source: 'unmatched' | 'refund' | 'skipped';
  }>({ open: false, transaction: null, transactionType: null, source: 'unmatched' });

  // Nominal posting details - maps row number to detail
  const [nominalPostingDetails, setNominalPostingDetails] = useState<Map<number, NominalPostingDetail>>(new Map());

  // Bank transfer modal state
  const [bankTransferModal, setBankTransferModal] = useState<{
    open: boolean;
    transaction: BankImportTransaction | null;
    source: 'unmatched' | 'refund' | 'skipped';
  }>({ open: false, transaction: null, source: 'unmatched' });

  // Bank transfer details - maps row number to full transfer info
  const [bankTransferDetails, setBankTransferDetails] = useState<Map<number, {
    destBankCode: string;
    destBankName: string;
    cashbookType: string;
    reference: string;
    comment: string;
    date: string;
  }>>(new Map());

  // Modal form state (at component level to avoid hooks-in-render issues)
  const [modalNominalCode, setModalNominalCode] = useState('');
  const [modalNominalSearch, setModalNominalSearch] = useState('');
  const [modalNominalDropdownOpen, setModalNominalDropdownOpen] = useState(false);
  const [modalNominalHighlightIndex, setModalNominalHighlightIndex] = useState(0);
  const [modalVatCode, setModalVatCode] = useState('');
  const [modalVatSearch, setModalVatSearch] = useState('');
  const [modalVatDropdownOpen, setModalVatDropdownOpen] = useState(false);
  const [modalVatHighlightIndex, setModalVatHighlightIndex] = useState(0);
  const [modalNetAmount, setModalNetAmount] = useState('');
  const [modalVatAmount, setModalVatAmount] = useState('');
  // Bank transfer modal fields
  const [modalDestBank, setModalDestBank] = useState('');
  const [modalDestBankSearch, setModalDestBankSearch] = useState('');
  const [modalDestBankDropdownOpen, setModalDestBankDropdownOpen] = useState(false);
  const [modalDestBankHighlightIndex, setModalDestBankHighlightIndex] = useState(0);
  const [modalCashbookType, setModalCashbookType] = useState('');
  const [modalReference, setModalReference] = useState('');
  const [modalComment, setModalComment] = useState('');
  const [modalDate, setModalDate] = useState('');

  // Refs for modal form focus management (fast keyboard entry)
  const modalVatInputRef = useRef<HTMLInputElement>(null);
  const modalNetAmountRef = useRef<HTMLInputElement>(null);
  const modalSaveButtonRef = useRef<HTMLButtonElement>(null);
  const modalDestBankInputRef = useRef<HTMLInputElement>(null);
  const modalBankTransferSaveRef = useRef<HTMLButtonElement>(null);

  // Inline account search state (for table dropdowns)
  const [inlineAccountSearch, setInlineAccountSearch] = useState<{ row: number; section: string } | null>(null);
  const [inlineAccountSearchText, setInlineAccountSearchText] = useState('');
  const [inlineAccountHighlightIndex, setInlineAccountHighlightIndex] = useState(0);

  // Save state to sessionStorage whenever key data changes (placed after all state declarations)
  useEffect(() => {
    // Don't save until restore is complete (prevents overwriting with empty state)
    if (!sessionRestoreComplete.current) return;

    if (bankPreview) {
      try {
        const toSave = {
          bankPreview,
          editedTransactions: mapToArray(editedTransactions), // Save unmatched transaction edits
          selectedForImport: Array.from(selectedForImport),
          dateOverrides: Array.from(dateOverrides.entries()),
          transactionTypeOverrides: Array.from(transactionTypeOverrides.entries()),
          includedSkipped: Array.from(includedSkipped.entries()),
          refundOverrides: Array.from(refundOverrides.entries()),
          nominalPostingDetails: mapToArray(nominalPostingDetails), // Save nominal posting details
          bankTransferDetails: mapToArray(bankTransferDetails), // Save bank transfer details
          autoAllocateDisabled: Array.from(autoAllocateDisabled), // Per-row auto-allocate disabled flags
          activePreviewTab,
          csvFileName,
          csvDirectory,
          selectedBankCode,
          // Persist source selections for import
          statementSource,
          selectedEmailStatement,
          selectedPdfFile,
        };
        sessionStorage.setItem(STORAGE_KEY, JSON.stringify(toSave));
      } catch (e) {
        console.warn('Failed to save bank import state to session storage:', e);
      }
    }
  }, [bankPreview, editedTransactions, selectedForImport, dateOverrides, transactionTypeOverrides, includedSkipped, refundOverrides, nominalPostingDetails, bankTransferDetails, autoAllocateDisabled, activePreviewTab, csvFileName, csvDirectory, selectedBankCode, statementSource, selectedEmailStatement, selectedPdfFile]);

  // Helper function to determine smart default transaction type for unmatched transactions
  // Defaults to nominal unless there's a pattern suggestion or clear customer/supplier hint
  const getSmartDefaultTransactionType = useCallback((txn: BankImportTransaction): TransactionType => {
    const isPositive = txn.amount > 0;

    // Check if there's a suggestion from pattern matching
    const suggestion = (txn as any);
    if (suggestion.suggested_type) {
      const typeMap: Record<string, TransactionType> = {
        'SI': 'sales_receipt', 'PI': 'purchase_payment',
        'SC': 'sales_refund', 'PC': 'purchase_refund',
        'NP': 'nominal_payment', 'NR': 'nominal_receipt',
        'BT': 'bank_transfer'
      };
      const mappedType = typeMap[suggestion.suggested_type];
      if (mappedType) return mappedType;
    }

    // Check if there's a suggested account (indicates customer/supplier match)
    if (suggestion.suggested_account && suggestion.suggested_ledger_type) {
      if (suggestion.suggested_ledger_type === 'C') {
        return isPositive ? 'sales_receipt' : 'sales_refund';
      } else {
        return isPositive ? 'purchase_refund' : 'purchase_payment';
      }
    }

    // Check if there's partial match info or a reason suggesting customer/supplier
    const name = (txn.name || '').toLowerCase();
    const memo = (txn.memo || '').toLowerCase();
    const reference = (txn.reference || '').toLowerCase();
    const combined = `${name} ${memo} ${reference}`;

    // FIRST: Check if any actual customer name appears in the transaction (for credits)
    if (isPositive && customers.length > 0) {
      for (const cust of customers) {
        const custName = (cust.name || '').toLowerCase();
        // Only match if customer name is at least 3 chars and appears in transaction
        if (custName.length >= 3 && combined.includes(custName)) {
          return 'sales_receipt';
        }
      }
    }

    // SECOND: Check if any actual supplier name appears in the transaction (for debits)
    if (!isPositive && suppliers.length > 0) {
      for (const supp of suppliers) {
        const suppName = (supp.name || '').toLowerCase();
        // Only match if supplier name is at least 3 chars and appears in transaction
        if (suppName.length >= 3 && combined.includes(suppName)) {
          return 'purchase_payment';
        }
      }
    }

    // Common patterns that suggest supplier payment (direct debits, standing orders)
    const supplierPatterns = [
      'dd ', 'direct debit', 'standing order', 'so ', 's/o',
      'payment to', 'to:', 'payee:', 'supplier',
      // Common UK utilities/services
      'virgin', 'bt ', 'sky ', 'vodafone', 'ee ', 'o2 ',
      'british gas', 'edf', 'eon', 'scottish power', 'npower',
      'water', 'electric', 'council', 'hmrc', 'vat',
      'insurance', 'rent', 'lease', 'mortgage',
      'amazon', 'ebay', 'paypal'
    ];

    // Common patterns that suggest customer receipt
    const customerPatterns = [
      'faster payment', 'bank giro credit', 'bgc',
      'transfer from', 'from:', 'payment from',
      'customer', 'client', 'inv ', 'invoice'
    ];

    // Check for supplier patterns (mostly for payments/debits)
    if (!isPositive) {
      for (const pattern of supplierPatterns) {
        if (combined.includes(pattern)) {
          return 'purchase_payment';
        }
      }
    }

    // Check for customer patterns (mostly for receipts/credits)
    if (isPositive) {
      for (const pattern of customerPatterns) {
        if (combined.includes(pattern)) {
          return 'sales_receipt';
        }
      }
    }

    // Default to nominal if no clear customer/supplier indication
    return isPositive ? 'nominal_receipt' : 'nominal_payment';
  }, [customers, suppliers]);

  // Bank account selector search state
  const [bankSelectSearch, setBankSelectSearch] = useState('');
  const [bankSelectOpen, setBankSelectOpen] = useState<string | null>(null); // 'email' | 'pdf' | 'csv' | null
  const [bankSelectHighlightIndex, setBankSelectHighlightIndex] = useState(0);

  // Fetch CSV files in the selected directory
  const { data: csvFilesData } = useQuery({
    queryKey: ['csv-files', csvDirectory],
    queryFn: async () => {
      const res = await authFetch(`${API_BASE}/bank-import/list-csv?directory=${encodeURIComponent(csvDirectory)}`);
      return res.json();
    },
    enabled: !!csvDirectory,
  });
  const csvFilesList = csvFilesData?.success ? csvFilesData.files : [];

  // Scan PDF files function - called on button click (mirrors email scan)
  const handleScanPdfFiles = async () => {
    if (!pdfDirectory) return;

    setPdfFilesLoading(true);
    setPdfFilesList(null);

    try {
      const res = await authFetch(
        `${API_BASE}/bank-import/list-pdf?directory=${encodeURIComponent(pdfDirectory)}&bank_code=${selectedBankCode}`
      );
      const data = await res.json();

      if (data.success) {
        setPdfFilesList(data.files || []);
      } else {
        setPdfFilesList([]);
      }
    } catch (err) {
      console.error('Failed to scan PDF files:', err);
      setPdfFilesList([]);
    } finally {
      setPdfFilesLoading(false);
    }
  };

  // Build full CSV file path from directory + filename
  const csvFilePath = csvDirectory && csvFileName
    ? (csvDirectory.endsWith('/') || csvDirectory.endsWith('\\')
        ? csvDirectory + csvFileName
        : csvDirectory + '/' + csvFileName)
    : csvFileName;

  // State for detected bank from file
  const [detectedBank, setDetectedBank] = useState<{
    detected: boolean;
    bank_code: string | null;
    bank_description: string;
    sort_code: string;
    account_number: string;
    message: string;
    loading: boolean;
  } | null>(null);

  // Auto-detect bank when file path changes
  useEffect(() => {
    const detectBank = async () => {
      if (!csvFilePath || !csvFilePath.trim()) {
        setDetectedBank(null);
        return;
      }

      setDetectedBank(prev => prev ? { ...prev, loading: true } : { detected: false, bank_code: null, bank_description: '', sort_code: '', account_number: '', message: 'Detecting...', loading: true });

      try {
        const response = await authFetch(`${API_BASE}/bank-import/detect-bank?filepath=${encodeURIComponent(csvFilePath)}`, {
          method: 'POST'
        });
        const data = await response.json();

        if (data.success && data.detected) {
          setDetectedBank({
            detected: true,
            bank_code: data.bank_code,
            bank_description: data.bank_description || data.bank_code,
            sort_code: data.sort_code || '',
            account_number: data.account_number || '',
            message: data.message || `Detected: ${data.bank_code}`,
            loading: false
          });
          // Auto-select the detected bank
          if (data.bank_code) {
            setSelectedBankCode(data.bank_code);
          }
        } else if (data.success && !data.detected) {
          setDetectedBank({
            detected: false,
            bank_code: null,
            bank_description: '',
            sort_code: '',
            account_number: '',
            message: data.message || 'Could not detect bank from file',
            loading: false
          });
        } else {
          setDetectedBank({
            detected: false,
            bank_code: null,
            bank_description: '',
            sort_code: '',
            account_number: '',
            message: data.error || 'Detection failed',
            loading: false
          });
        }
      } catch (error) {
        setDetectedBank({
          detected: false,
          bank_code: null,
          bank_description: '',
          sort_code: '',
          account_number: '',
          message: error instanceof Error ? error.message : 'Detection error',
          loading: false
        });
      }
    };

    detectBank();
  }, [csvFilePath]);

  // Persist CSV directory to localStorage
  useEffect(() => {
    if (csvDirectory) {
      localStorage.setItem('bankImport_csvDirectory', csvDirectory);
    }
  }, [csvDirectory]);

  // Save bank code to company-specific localStorage key
  useEffect(() => {
    if (selectedBankCode && currentCompanyId) {
      localStorage.setItem(`bankImport_bankCode_${currentCompanyId}`, selectedBankCode);
    }
  }, [selectedBankCode, currentCompanyId]);

  useEffect(() => {
    if (opera3DataPath) {
      localStorage.setItem('bankImport_opera3DataPath', opera3DataPath);
    }
  }, [opera3DataPath]);

  // Persist PDF directory to localStorage
  useEffect(() => {
    if (pdfDirectory) {
      localStorage.setItem('bankImport_pdfDirectory', pdfDirectory);
    }
  }, [pdfDirectory]);

  // Fetch bank accounts using react-query (auto-refreshes on company switch)
  const { data: bankAccountsData } = useQuery({
    queryKey: ['bank-accounts'],
    queryFn: async () => {
      const res = await authFetch(`${API_BASE}/opera-sql/bank-accounts`);
      return res.json();
    },
  });

  // Track previous company to detect company switches
  // Use null to distinguish "never set" from "empty string"
  const previousCompanyRef = useRef<string | null>(null);
  const hasInitializedBankCode = useRef<boolean>(false);

  // Update bank accounts state when data changes or company changes
  useEffect(() => {
    if (bankAccountsData?.success && bankAccountsData.bank_accounts && currentCompanyId) {
      const accounts = bankAccountsData.bank_accounts.map((b: any) => ({
        code: b.code,
        description: b.description,
        sort_code: b.sort_code || '',
        account_number: b.account_number || ''
      }));
      setBankAccounts(accounts);

      // Detect if company has ACTUALLY changed (not initial load)
      // previousCompanyRef.current === null means this is first load
      const previousCompany = previousCompanyRef.current;
      const isInitialLoad = previousCompany === null;
      const companyChanged = !isInitialLoad && previousCompany !== currentCompanyId;

      // Update ref after checking
      previousCompanyRef.current = currentCompanyId;

      // Only set bank code on initial load or company change
      // BUT skip if we restored from session (session bank code takes priority)
      if ((!hasInitializedBankCode.current || companyChanged) && !hasRestoredFromSession.current) {
        hasInitializedBankCode.current = true;

        // Load bank code from company-specific localStorage key
        const savedBankCode = localStorage.getItem(`bankImport_bankCode_${currentCompanyId}`);
        const savedBankCodeValid = savedBankCode ? accounts.some((a: BankAccount) => a.code === savedBankCode) : false;

        if (savedBankCodeValid) {
          setSelectedBankCode(savedBankCode!);
        } else if (accounts.length > 0) {
          setSelectedBankCode(accounts[0].code);
        }
      } else if (hasRestoredFromSession.current) {
        // Mark as initialized even if we skipped (session had the value)
        hasInitializedBankCode.current = true;
      }

      // Clear ALL reconciliation state ONLY when company actually changes (NOT on initial load)
      if (companyChanged) {
        console.log(`Company changed from ${previousCompany} to ${currentCompanyId} - clearing all reconciliation state`);
        // Clear bank preview and transaction state
        setBankPreview(null);
        setBankImportResult(null);
        setEditedTransactions(new Map());
        setSelectedForImport(new Set());
        setIncludedSkipped(new Map());
        setTransactionTypeOverrides(new Map());
        setRefundOverrides(new Map());
        setAutoAllocateDisabled(new Set());
        setDateOverrides(new Map());
        setNominalPostingDetails(new Map());
        setBankTransferDetails(new Map());
        setUpdatedRepeatEntries(new Set());
        // Clear email/PDF selections
        setSelectedEmailStatement(null);
        setEmailStatements([]);
        setSelectedPdfFile(null);
        setPdfFilesList([]);
        // Clear file selections
        setCsvFileName('');
        // Clear detected bank
        setDetectedBank(null);
        // Clear UI state
        setShowRawPreview(false);
        setRawFilePreview(null);
        setPdfViewerData(null);
        setShowReconcilePrompt(false);
        setShowImportHistory(false);
        setImportHistoryData([]);
        // Reset tabs
        setActivePreviewTab('receipts');
        setTabSearchFilter('');
        // Clear session storage for old company
        clearPersistedState();
      }
    }
  }, [bankAccountsData, currentCompanyId]);

  // Fetch unreconciled entries when reconcile prompt is shown
  useEffect(() => {
    if (showReconcilePrompt && selectedBankCode && bankImportResult?.success) {
      const fetchUnreconciled = async () => {
        setLoadingUnreconciled(true);
        try {
          const res = await authFetch(`${API_BASE}/bank-reconciliation/unreconciled-entries?bank_code=${selectedBankCode}`);
          const data = await res.json();
          if (data.success && data.entries) {
            setUnreconciledEntries(data.entries);
          }
        } catch (err) {
          console.error('Failed to fetch unreconciled entries:', err);
        } finally {
          setLoadingUnreconciled(false);
        }
      };
      fetchUnreconciled();
    }
  }, [showReconcilePrompt, selectedBankCode, bankImportResult?.success]);

  // Query for reconciliation-in-progress status
  const { data: reconciliationStatus } = useQuery({
    queryKey: ['bank-reconciliation-status', selectedBankCode, dataSource, opera3DataPath],
    queryFn: async () => {
      if (!selectedBankCode) return null;
      if (dataSource === 'opera3') {
        if (!opera3DataPath) return null;
        const res = await authFetch(`${API_BASE}/opera3/reconcile/bank/${selectedBankCode}/status?data_path=${encodeURIComponent(opera3DataPath)}`);
        return res.json();
      } else {
        const res = await authFetch(`${API_BASE}/reconcile/bank/${selectedBankCode}/status`);
        return res.json();
      }
    },
    enabled: !!selectedBankCode && (dataSource !== 'opera3' || !!opera3DataPath),
    refetchOnWindowFocus: true,
    staleTime: 30000, // 30 seconds
  });

  // =====================
  // IMPORT HISTORY FUNCTIONS
  // =====================

  // Fetch import history - uses Opera 3 endpoint if configured
  const fetchImportHistory = useCallback(async (limit: number = historyLimit, fromDate?: string, toDate?: string) => {
    setImportHistoryLoading(true);
    try {
      const params = new URLSearchParams({ limit: String(limit) });
      if (fromDate) params.append('from_date', fromDate);
      if (toDate) params.append('to_date', toDate);
      const historyUrl = dataSource === 'opera3'
        ? `/api/opera3/bank-import/import-history?${params}`
        : `/api/bank-import/import-history?${params}`;
      const response = await authFetch(historyUrl);
      const data = await response.json();
      if (data.success) {
        setImportHistoryData(data.imports || []);
      }
    } catch (error) {
      console.error('Failed to fetch import history:', error);
    } finally {
      setImportHistoryLoading(false);
    }
  }, [dataSource, historyLimit]);

  // Clear import history
  const clearImportHistory = async () => {
    setShowClearConfirm(false);
    setIsClearing(true);
    try {
      const params = new URLSearchParams();
      if (historyFromDate) params.append('from_date', historyFromDate);
      if (historyToDate) params.append('to_date', historyToDate);
      const url = dataSource === 'opera3'
        ? `/api/opera3/bank-import/import-history?${params}`
        : `/api/bank-import/import-history?${params}`;
      const response = await authFetch(url, { method: 'DELETE' });
      const data = await response.json();
      if (data.success) {
        alert(`Cleared ${data.deleted_count} records`);
        fetchImportHistory(historyLimit, historyFromDate, historyToDate);
      } else {
        alert(`Error: ${data.error}`);
      }
    } catch (error) {
      console.error('Failed to clear history:', error);
      alert('Failed to clear history');
    } finally {
      setIsClearing(false);
    }
  };

  // Delete single history record to allow re-import
  const deleteHistoryRecord = async () => {
    if (!reImportRecord) return;
    setIsDeleting(true);
    try {
      const url = dataSource === 'opera3'
        ? `/api/opera3/bank-import/import-history/${reImportRecord.id}`
        : `/api/bank-import/import-history/${reImportRecord.id}`;
      const response = await authFetch(url, { method: 'DELETE' });
      const data = await response.json();
      if (data.success) {
        fetchImportHistory(historyLimit, historyFromDate, historyToDate);
        setReImportRecord(null);
      } else {
        alert(`Error: ${data.error}`);
      }
    } catch (error) {
      console.error('Failed to delete history record:', error);
      alert('Failed to delete history record');
    } finally {
      setIsDeleting(false);
    }
  };

  // Load history when modal opens
  useEffect(() => {
    if (showImportHistory) {
      fetchImportHistory(historyLimit, historyFromDate, historyToDate);
    }
  }, [showImportHistory, fetchImportHistory, historyLimit, historyFromDate, historyToDate]);

  // Common fields
  const [bankAccount, setBankAccount] = useState('BC010');
  const [postDate, setPostDate] = useState(new Date().toISOString().split('T')[0]);
  const [inputBy, setInputBy] = useState('IMPORT');
  const [reference, setReference] = useState('');

  // Sales Receipt fields
  const [customerAccount, setCustomerAccount] = useState('');
  const [receiptAmount, setReceiptAmount] = useState('');

  // Purchase Payment fields
  const [supplierAccount, setSupplierAccount] = useState('');
  const [paymentAmount, setPaymentAmount] = useState('');

  // Invoice fields
  const [invoiceNumber, setInvoiceNumber] = useState('');
  const [netAmount, setNetAmount] = useState('');
  const [vatAmount, setVatAmount] = useState('');
  const [nominalAccount, setNominalAccount] = useState('');
  const [description, setDescription] = useState('');

  // Nominal Journal fields
  const [journalLines, setJournalLines] = useState([
    { account: '', amount: '', description: '' },
    { account: '', amount: '', description: '' }
  ]);

  const resetForm = () => {
    setResult(null);
    setReference('');
    setCustomerAccount('');
    setReceiptAmount('');
    setSupplierAccount('');
    setPaymentAmount('');
    setInvoiceNumber('');
    setNetAmount('');
    setVatAmount('');
    setDescription('');
    setJournalLines([
      { account: '', amount: '', description: '' },
      { account: '', amount: '', description: '' }
    ]);
    // Bank statement reset - keep csvFilePath as it's persisted
    setBankPreview(null);
    setBankImportResult(null);
    clearPersistedState(); // Clear sessionStorage when form is reset
  };

  // Bank statement preview with enhanced format detection
  const handleBankPreview = async () => {
    setIsPreviewing(true);
    setBankPreview(null);
    setRawFilePreview(null);
    setShowRawPreview(false);
    setBankImportResult(null);
    setEditedTransactions(new Map());
    setIncludedSkipped(new Map());
    setTransactionTypeOverrides(new Map());
    setRefundOverrides(new Map());
    setTabSearchFilter('');
    try {
      let url: string;
      const isPdfFile = csvFilePath.toLowerCase().endsWith('.pdf');

      if (dataSource === 'opera-sql') {
        // Check if it's a PDF file - route to PDF endpoint for AI extraction
        if (isPdfFile) {
          url = `${API_BASE}/bank-import/preview-from-pdf?file_path=${encodeURIComponent(csvFilePath)}&bank_code=${selectedBankCode}`;
        } else {
          // Use enhanced multi-format preview for CSV/OFX/QIF/MT940
          url = `${API_BASE}/bank-import/preview-multiformat?filepath=${encodeURIComponent(csvFilePath)}&bank_code=${selectedBankCode}`;
        }
      } else {
        // Opera 3 data source
        if (isPdfFile) {
          url = `${API_BASE}/opera3/bank-import/preview-from-pdf?file_path=${encodeURIComponent(csvFilePath)}&data_path=${encodeURIComponent(opera3DataPath)}&bank_code=${selectedBankCode}`;
        } else {
          url = `${API_BASE}/opera3/bank-import/preview?filepath=${encodeURIComponent(csvFilePath)}&data_path=${encodeURIComponent(opera3DataPath)}`;
        }
      }
      const response = await authFetch(url, { method: 'POST' });
      const data = await response.json();

      // Check for bank mismatch error
      if (data.bank_mismatch) {
        setBankPreview({
          success: false,
          filename: csvFilePath,
          total_transactions: 0,
          matched_receipts: [],
          matched_payments: [],
          matched_refunds: [],
          repeat_entries: [],
          unmatched: [],
          already_posted: [],
          skipped: [],
          errors: [
            `Bank account mismatch: The CSV file is for bank ${data.detected_bank}, but you selected ${data.selected_bank}.`,
            'Please select the correct bank account and try again.'
          ]
        });
        return;
      }

      // Handle statement sequence validation responses
      if (data.status === 'skipped') {
        // This statement has already been processed (opening balance < reconciled balance)
        setBankPreview({
          success: false,
          filename: csvFilePath,
          total_transactions: 0,
          matched_receipts: [],
          matched_payments: [],
          matched_refunds: [],
          repeat_entries: [],
          unmatched: [],
          already_posted: [],
          skipped: [],
          errors: [
            `Statement already processed or superseded.`,
            `The statement opening balance (£${data.statement_info?.opening_balance?.toFixed(2) || '?'}) is less than Opera's reconciled balance (£${data.reconciled_balance?.toFixed(2) || '?'}).`,
            `This statement covers a period that has already been reconciled (possibly manually).`
          ]
        });
        return;
      }

      if (data.status === 'pending') {
        // Future statement - missing one in between
        setBankPreview({
          success: false,
          filename: csvFilePath,
          total_transactions: 0,
          matched_receipts: [],
          matched_payments: [],
          matched_refunds: [],
          repeat_entries: [],
          unmatched: [],
          already_posted: [],
          skipped: [],
          errors: [
            `Statement out of sequence - missing earlier statement.`,
            `Statement opening balance: £${data.statement_info?.opening_balance?.toFixed(2) || '?'}`,
            `Opera reconciled balance: £${data.reconciled_balance?.toFixed(2) || '?'}`,
            `Please import the missing statement(s) first, or manually reconcile to £${data.statement_info?.opening_balance?.toFixed(2) || '?'} in Opera.`
          ]
        });
        return;
      }

      // Handle enhanced response format
      // Determine default format based on file extension if backend doesn't specify
      const defaultFormat = isPdfFile ? 'PDF' : 'CSV';
      const enhancedPreview: EnhancedBankImportPreview = {
        success: data.success,
        filename: data.filename,
        detected_format: data.detected_format || defaultFormat,
        total_transactions: data.total_transactions,
        matched_receipts: data.matched_receipts || [],
        matched_payments: data.matched_payments || [],
        matched_refunds: data.matched_refunds || [],
        repeat_entries: data.repeat_entries || [],
        unmatched: data.unmatched || [],
        already_posted: data.already_posted || [],
        skipped: data.skipped || [],
        summary: data.summary,
        errors: data.errors || (data.error ? [data.error] : []),
        // Include statement bank info from AI extraction (for PDF statements)
        statement_bank_info: data.statement_bank_info ? {
          bank_name: data.statement_bank_info.bank_name,
          account_number: data.statement_bank_info.account_number,
          sort_code: data.statement_bank_info.sort_code,
          statement_date: data.statement_bank_info.statement_date,
          opening_balance: data.statement_bank_info.opening_balance,
          closing_balance: data.statement_bank_info.closing_balance,
          matched_opera_bank: data.statement_bank_info.matched_opera_bank,
          matched_opera_name: data.statement_bank_info.matched_opera_name
        } : undefined
      };

      setBankPreview(enhancedPreview);

      // Initialize selectedForImport - auto-select all items with complete data (not duplicates)
      const preSelected = new Set<number>();
      // Receipts - always have account, select if not duplicate
      enhancedPreview.matched_receipts.filter(t => !t.is_duplicate).forEach(t => preSelected.add(t.row));
      // Payments - always have account, select if not duplicate
      enhancedPreview.matched_payments.filter(t => !t.is_duplicate).forEach(t => preSelected.add(t.row));
      // Refunds - have account from matching, select if not duplicate
      (enhancedPreview.matched_refunds || []).filter(t => !t.is_duplicate).forEach(t => preSelected.add(t.row));
      // Repeat entries - NOT pre-selected (handled separately by Opera)

      // Clear any previous state before applying suggestions
      const newEditedTransactions = new Map<number, BankImportTransaction>();
      const newTransactionTypeOverrides = new Map<number, TransactionType>();
      const newIncludedSkipped = new Map<number, { account: string; ledger_type: 'C' | 'S'; transaction_type: TransactionType }>();
      const newNominalPostingDetails = new Map<number, NominalPostingDetail>();

      // Apply suggestions to UNMATCHED transactions
      for (const txn of enhancedPreview.unmatched) {
        const suggestion = (txn as any);
        if (suggestion.suggested_account && suggestion.suggested_ledger_type) {
          // Pre-fill the account from suggestion
          newEditedTransactions.set(txn.row, {
            ...txn,
            manual_account: suggestion.suggested_account,
            manual_ledger_type: suggestion.suggested_ledger_type,
            account_name: suggestion.suggested_account_name || '',
            isEdited: true
          });

          // Set transaction type override if suggested
          if (suggestion.suggested_type) {
            // Map backend codes to frontend TransactionType
            const typeMap: Record<string, TransactionType> = {
              'SI': 'sales_receipt', 'PI': 'purchase_payment',
              'SC': 'sales_refund', 'PC': 'purchase_refund',
              'NP': 'nominal_payment', 'NR': 'nominal_receipt',
              'BT': 'bank_transfer'
            };
            const mappedType = typeMap[suggestion.suggested_type];
            if (mappedType) {
              newTransactionTypeOverrides.set(txn.row, mappedType);
            }
          }

          // If nominal type with VAT suggestion, set nominal posting details
          if (suggestion.suggested_type === 'NP' || suggestion.suggested_type === 'NR') {
            if (suggestion.suggested_nominal_code) {
              const grossAmount = Math.abs(txn.amount);
              const vatCode = suggestion.suggested_vat_code || 'N/A';
              // Default to 0% VAT rate if not available
              const vatRate = 0;
              newNominalPostingDetails.set(txn.row, {
                nominalCode: suggestion.suggested_nominal_code,
                vatCode: vatCode,
                vatRate: vatRate,
                netAmount: grossAmount,
                vatAmount: 0,
                grossAmount: grossAmount
              });
            }
          }

          // Auto-select for import since we have complete data
          if (!txn.is_duplicate) {
            preSelected.add(txn.row);
          }
        }
      }

      // Apply suggestions to SKIPPED transactions (if they have suggestions)
      for (const txn of enhancedPreview.skipped) {
        const suggestion = (txn as any);
        if (suggestion.suggested_account && suggestion.suggested_ledger_type) {
          // Determine the transaction type based on amount and suggested type
          let transactionType: TransactionType = txn.amount > 0 ? 'sales_receipt' : 'purchase_payment';
          if (suggestion.suggested_type) {
            const typeMap: Record<string, TransactionType> = {
              'SI': 'sales_receipt', 'PI': 'purchase_payment',
              'SC': 'sales_refund', 'PC': 'purchase_refund',
              'NP': 'nominal_payment', 'NR': 'nominal_receipt',
              'BT': 'bank_transfer'
            };
            transactionType = typeMap[suggestion.suggested_type] || transactionType;
          }

          // Include in skipped with pre-filled data
          newIncludedSkipped.set(txn.row, {
            account: suggestion.suggested_account,
            ledger_type: suggestion.suggested_ledger_type,
            transaction_type: transactionType
          });

          // Auto-select for import
          if (!txn.is_duplicate) {
            preSelected.add(txn.row);
          }
        }
      }

      setSelectedForImport(preSelected);

      // Apply the pre-filled data
      setEditedTransactions(newEditedTransactions);
      setTransactionTypeOverrides(newTransactionTypeOverrides);
      setNominalPostingDetails(newNominalPostingDetails);
      setIncludedSkipped(newIncludedSkipped);

      // Clear remaining state
      setDateOverrides(new Map());
      setRefundOverrides(new Map());
      setUpdatedRepeatEntries(new Set());

      // Auto-select best tab
      if (enhancedPreview.matched_receipts.length > 0) setActivePreviewTab('receipts');
      else if (enhancedPreview.matched_payments.length > 0) setActivePreviewTab('payments');
      else if (enhancedPreview.matched_refunds?.length > 0) setActivePreviewTab('refunds');
      else if (enhancedPreview.repeat_entries?.length > 0) setActivePreviewTab('repeat');
      else if (enhancedPreview.unmatched.length > 0) setActivePreviewTab('unmatched');
      else setActivePreviewTab('skipped');
    } catch (error) {
      setBankPreview({
        success: false,
        filename: csvFilePath,
        total_transactions: 0,
        matched_receipts: [],
        matched_payments: [],
        matched_refunds: [],
        repeat_entries: [],
        unmatched: [],
        already_posted: [],
        skipped: [],
        errors: [error instanceof Error ? error.message : 'Unknown error']
      });
    } finally {
      setIsPreviewing(false);
    }
  };

  // Preview raw file contents (first 50 lines) - works for all source types
  const handleRawFilePreview = async () => {
    try {
      let response;

      if (statementSource === 'email' && selectedEmailStatement) {
        // Email source - use email attachment preview endpoint
        response = await authFetch(`${API_BASE}/bank-import/raw-preview-email?email_id=${selectedEmailStatement.emailId}&attachment_id=${encodeURIComponent(selectedEmailStatement.attachmentId)}&lines=50`);
        const data = await response.json();
        if (data.success) {
          if (data.is_pdf && data.pdf_data) {
            // PDF - show in PDF viewer popup
            setPdfViewerData({ data: data.pdf_data, filename: data.filename || 'document.pdf' });
          } else {
            setRawFilePreview(data.lines);
            setShowRawPreview(true);
          }
        } else {
          setRawFilePreview([`Error: ${data.error || 'Failed to read attachment'}`]);
          setShowRawPreview(true);
        }
      } else if (statementSource === 'pdf' && selectedPdfFile) {
        // PDF source - open the PDF file in viewer
        response = await authFetch(`${API_BASE}/bank-import/pdf-content?filename=${encodeURIComponent(selectedPdfFile.filename)}`);
        const data = await response.json();
        if (data.success && data.pdf_data) {
          setPdfViewerData({ data: data.pdf_data, filename: selectedPdfFile.filename });
        } else {
          setRawFilePreview([`Error: ${data.error || 'Failed to read PDF'}`]);
          setShowRawPreview(true);
        }
      } else if (csvFilePath) {
        // File source - use raw preview endpoint
        response = await authFetch(`${API_BASE}/bank-import/raw-preview?filepath=${encodeURIComponent(csvFilePath)}&lines=50`);
        const data = await response.json();
        if (data.success) {
          setRawFilePreview(data.lines);
          setShowRawPreview(true);
        } else {
          setRawFilePreview([`Error: ${data.error || 'Failed to read file'}`]);
          setShowRawPreview(true);
        }
      }
    } catch (error) {
      setRawFilePreview([`Error: ${error instanceof Error ? error.message : 'Failed to read file'}`]);
      setShowRawPreview(true);
    }
  };

  // Preview raw email attachment contents (first 50 lines) or PDF in popup
  const handleEmailAttachmentRawPreview = async (emailId: number, attachmentId: string) => {
    try {
      const response = await authFetch(`${API_BASE}/bank-import/raw-preview-email?email_id=${emailId}&attachment_id=${encodeURIComponent(attachmentId)}&lines=50`);
      const data = await response.json();
      if (data.success) {
        // If it's a PDF, show in PDF viewer popup
        if (data.is_pdf && data.pdf_data) {
          setPdfViewerData({ data: data.pdf_data, filename: data.filename || 'document.pdf' });
        } else {
          setRawFilePreview(data.lines);
          setShowRawPreview(true);
        }
      } else {
        setRawFilePreview([`Error: ${data.error || 'Failed to read attachment'}`]);
        setShowRawPreview(true);
      }
    } catch (error) {
      setRawFilePreview([`Error: ${error instanceof Error ? error.message : 'Failed to read attachment'}`]);
      setShowRawPreview(true);
    }
  };

  // Handle account change for a transaction
  const handleAccountChange = useCallback((txn: BankImportTransaction, accountCode: string, ledgerType: 'C' | 'S' | 'N') => {
    const updated = new Map(editedTransactions);
    let accountName = '';

    if (ledgerType === 'C') {
      accountName = customers.find(c => c.code === accountCode)?.name || '';
    } else if (ledgerType === 'S') {
      accountName = suppliers.find(s => s.code === accountCode)?.name || '';
    } else if (ledgerType === 'N') {
      accountName = nominalAccounts.find(n => n.code === accountCode)?.description || '';
    }

    updated.set(txn.row, {
      ...txn,
      manual_account: accountCode,
      manual_ledger_type: ledgerType as 'C' | 'S',  // Cast for type compatibility - N is handled specially
      account_name: accountName,
      isEdited: true
    });
    setEditedTransactions(updated);

    // Auto-select for import when account is assigned
    setSelectedForImport(prev => new Set(prev).add(txn.row));
  }, [editedTransactions, customers, suppliers, nominalAccounts]);

  // Suggest account based on transaction name and type
  const suggestAccountForTransaction = useCallback(async (txn: BankImportTransaction, transactionType: TransactionType) => {
    // Only suggest for customer/supplier types, not nominal or bank transfer
    const isCustomerType = transactionType === 'sales_receipt' || transactionType === 'sales_refund';
    const isSupplierType = transactionType === 'purchase_payment' || transactionType === 'purchase_refund';

    if (!isCustomerType && !isSupplierType) return;

    const searchName = txn.name || txn.reference || '';
    if (!searchName.trim()) return;

    try {
      const response = await authFetch(
        `${API_BASE}/bank-import/suggest-account?name=${encodeURIComponent(searchName)}&transaction_type=${transactionType}&limit=1`
      );
      const data = await response.json();

      if (data.success && data.suggestions && data.suggestions.length > 0) {
        const suggestion = data.suggestions[0];
        // Only auto-apply if confidence is high enough (>= 70%)
        if (suggestion.score >= 70) {
          handleAccountChange(txn, suggestion.code, data.ledger_type as 'C' | 'S');
        }
      }
    } catch (error) {
      console.error('Error suggesting account:', error);
    }
  }, [authFetch, handleAccountChange]);

  // Note: handleRowSelect and handleBulkAssign removed - will be re-added when bulk operations feature is implemented

  // Calculate import readiness - which transactions are selected AND have all mandatory data
  const importReadiness = (() => {
    if (!bankPreview) return null;

    // Matched receipts - selected and have account
    const receiptsSelected = (bankPreview.matched_receipts || []).filter(t => selectedForImport.has(t.row) && !t.is_duplicate);
    const receiptsReady = receiptsSelected.length;
    const receiptsTotal = (bankPreview.matched_receipts || []).length;

    // Matched payments - selected and have account
    const paymentsSelected = (bankPreview.matched_payments || []).filter(t => selectedForImport.has(t.row) && !t.is_duplicate);
    const paymentsReady = paymentsSelected.length;
    const paymentsTotal = (bankPreview.matched_payments || []).length;

    // Refunds - selected and have account
    const refunds = bankPreview.matched_refunds || [];
    const refundsSelected = refunds.filter(t => {
      if (!selectedForImport.has(t.row) || t.is_duplicate) return false;
      const override = refundOverrides.get(t.row);
      // Has account (either matched or overridden)
      const hasAccount = t.account || override?.account;
      return hasAccount;
    });
    const refundsReady = refundsSelected.length;
    const refundsTotal = refunds.length;

    // Unmatched - selected and have account assigned (all types now require account selection)
    // Filter out ignored transactions from unmatched
    const unmatchedNotIgnored = (bankPreview.unmatched || []).filter(t => !ignoredTransactions.has(t.row));
    const unmatchedSelected = unmatchedNotIgnored.filter(t => selectedForImport.has(t.row));
    const unmatchedWithAccount = unmatchedSelected.filter(t => {
      const editedTxn = editedTransactions.get(t.row);
      const currentTxnType = transactionTypeOverrides.get(t.row) || getSmartDefaultTransactionType(t);
      const isNominal = currentTxnType === 'nominal_receipt' || currentTxnType === 'nominal_payment';
      const isBankTransfer = currentTxnType === 'bank_transfer';
      const isNlOrTransfer = isNominal || isBankTransfer;
      // For Nominal/Bank Transfer, account is handled elsewhere (nominalPostingDetails/bankTransferDetails)
      return isNlOrTransfer || editedTxn?.manual_account;
    });
    const unmatchedReady = unmatchedWithAccount.length;
    const unmatchedIncomplete = unmatchedSelected.length - unmatchedReady; // Selected but missing required account
    const unmatchedTotal = unmatchedNotIgnored.length;

    // Skipped included - selected (via includedSkipped) and have account assigned
    const skippedIncluded = includedSkipped.size;
    const skippedWithAccount = Array.from(includedSkipped.entries()).filter(([, v]) => {
      return v.account;
    });
    const skippedReady = skippedWithAccount.length;
    const skippedIncomplete = skippedIncluded - skippedReady;

    const totalReady = receiptsReady + paymentsReady + refundsReady + unmatchedReady + skippedReady;
    const totalIncomplete = unmatchedIncomplete + skippedIncomplete; // Items selected but missing account

    // Count period violations for selected transactions (that haven't been fixed with date overrides)
    const allSelectedTransactions = [
      ...receiptsSelected,
      ...paymentsSelected,
      ...refundsSelected,
      ...unmatchedWithAccount,
      ...Array.from(includedSkipped.keys()).map(row => {
        const skipped = (bankPreview.skipped || []).find(t => t.row === row);
        return skipped;
      }).filter(Boolean) as BankImportTransaction[]
    ];

    const periodViolationsCount = allSelectedTransactions.filter(t => {
      // Check if this transaction has a period violation and hasn't been fixed
      if (!t.period_valid && t.period_error) {
        // Check if user has provided a date override
        return !dateOverrides.has(t.row);
      }
      return false;
    }).length;

    // Count unhandled repeat entries - these must be processed in Opera before importing
    const repeatEntries = bankPreview.repeat_entries || [];
    const unhandledRepeatEntries = repeatEntries.filter(t =>
      !updatedRepeatEntries.has(t.repeat_entry_ref || '')
    ).length;
    const hasUnhandledRepeatEntries = unhandledRepeatEntries > 0;

    // Debug logging
    console.log('Import readiness:', {
      receiptsReady, paymentsReady, refundsReady, unmatchedReady, skippedReady,
      totalReady, totalIncomplete, unmatchedIncomplete, skippedIncomplete,
      periodViolationsCount, unhandledRepeatEntries,
      ignoredCount: ignoredTransactions.size,
      unmatchedTotal: (bankPreview.unmatched || []).length,
      unmatchedNotIgnoredCount: unmatchedNotIgnored.length,
      unmatchedSelectedCount: unmatchedSelected.length
    });

    return {
      receiptsReady, receiptsTotal,
      paymentsReady, paymentsTotal,
      refundsReady, refundsTotal,
      unmatchedReady, unmatchedTotal, unmatchedIncomplete,
      skippedReady, skippedIncluded, skippedIncomplete,
      totalReady,
      totalIncomplete,
      periodViolationsCount,
      hasPeriodViolations: periodViolationsCount > 0,
      repeatEntriesTotal: repeatEntries.length,
      unhandledRepeatEntries,
      hasUnhandledRepeatEntries,
      canImport: totalReady > 0 && totalIncomplete === 0 && periodViolationsCount === 0 && !hasUnhandledRepeatEntries
    };
  })();

  // Computed import state variables (used in both top button bar and bottom import section)
  const isEmailSource = statementSource === 'email';
  const isPdfSource = statementSource === 'pdf';
  const bankReady = (isEmailSource || isPdfSource) ? !!selectedBankCode : (detectedBank?.detected || selectedBankCode);
  const noBankSelected = !bankReady;
  const noPreview = !bankPreview;
  const hasIncomplete = !!(importReadiness?.totalIncomplete && importReadiness.totalIncomplete > 0);
  const hasNothingToImport = !!(importReadiness && importReadiness.totalReady === 0);
  const hasPeriodViolations = !!(importReadiness?.hasPeriodViolations);
  const hasUnhandledRepeatEntries = !!(importReadiness?.hasUnhandledRepeatEntries);
  const importDisabled = isImporting || dataSource === 'opera3' || noBankSelected || noPreview || hasIncomplete || hasNothingToImport || hasPeriodViolations || hasUnhandledRepeatEntries;

  // Build tooltip message for import button
  const importTitle = (() => {
    if (noBankSelected) return isEmailSource ? 'Select a bank account first' : isPdfSource ? 'Select a bank account first' : 'Please select a CSV file first to detect the bank account';
    if (noPreview) return isEmailSource ? 'Select an email attachment to preview' : isPdfSource ? 'Select a PDF file to preview' : 'Run Analyse Transactions first to review';
    if (dataSource === 'opera3') return 'Import not available for Opera 3 (read-only)';
    if (hasUnhandledRepeatEntries) return 'Cannot import - update repeat entry dates, run Opera Recurring Entries, then re-preview';
    if (hasPeriodViolations) return 'Cannot import - some transactions have dates outside the allowed posting period. Correct the dates below.';
    if (hasIncomplete) return 'Cannot import - some included items are missing required account assignment';
    if (hasNothingToImport) return 'No transactions ready to import';
    return '';
  })();

  // Bank statement import with manual overrides
  const handleBankImport = async () => {
    setIsImporting(true);
    setBankImportResult(null);

    try {
      if (dataSource === 'opera3') {
        setBankImportResult({
          success: false,
          error: 'Import not available for Opera 3. Opera 3 data is read-only.'
        });
        setIsImporting(false);
        return;
      }

      // Prepare overrides from edited transactions (unmatched)
      const unmatchedOverrides = Array.from(editedTransactions.values())
        .filter(txn => txn.manual_account && selectedForImport.has(txn.row))
        .map(txn => ({
          row: txn.row,
          account: txn.manual_account,
          ledger_type: txn.manual_ledger_type,
          transaction_type: transactionTypeOverrides.get(txn.row) || (txn.manual_ledger_type === 'C' ? 'sales_receipt' : 'purchase_payment')
        }));

      // Prepare overrides from included skipped items (only those with accounts assigned)
      const skippedOverrides = Array.from(includedSkipped.entries())
        .filter(([, data]) => data.account)
        .map(([row, data]) => ({
          row,
          account: data.account,
          ledger_type: data.ledger_type,
          transaction_type: data.transaction_type
        }));

      // Prepare overrides from modified refunds (changed type/account)
      const refundOverridesList = Array.from(refundOverrides.entries())
        .filter(([row, data]) => selectedForImport.has(row) && (data.transaction_type || data.account))
        .map(([row, data]) => ({
          row,
          account: data.account,
          ledger_type: data.ledger_type,
          transaction_type: data.transaction_type
        }));

      const overrides = [...unmatchedOverrides, ...skippedOverrides, ...refundOverridesList];

      // Convert selectedForImport to array for the API
      const selectedRowsArray = Array.from(selectedForImport);

      // Convert date overrides to array for the API
      const dateOverridesList = Array.from(dateOverrides.entries()).map(([row, date]) => ({
        row,
        date
      }));

      // Always use import-with-overrides endpoint with selected rows
      // Include per-row auto-allocate disabled flags - only send rows that are selected AND have auto-allocate disabled
      const autoAllocateDisabledRows = Array.from(autoAllocateDisabled).filter(row => selectedRowsArray.includes(row));

      const url = `${API_BASE}/bank-import/import-with-overrides?filepath=${encodeURIComponent(csvFilePath)}&bank_code=${selectedBankCode}&auto_allocate=${autoAllocate}`;
      const options: RequestInit = {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          overrides,
          selected_rows: selectedRowsArray,
          date_overrides: dateOverridesList,
          auto_allocate_disabled_rows: autoAllocateDisabledRows  // Rows to skip auto-allocation even if global flag is on
        })
      };

      const response = await authFetch(url, options);
      const data = await response.json();
      setBankImportResult(data);

      // Clear edited transactions after successful import but keep bankPreview for summary
      if (data.success) {
        setEditedTransactions(new Map());
        setIncludedSkipped(new Map());
        setTransactionTypeOverrides(new Map());
        setRefundOverrides(new Map());
        setSelectedForImport(new Set());
        setDateOverrides(new Map());
        setAutoAllocateDisabled(new Set());
        // Note: Do NOT clear bankPreview - keep it visible for summary until user clicks "Clear Statement"
        // Note: Do NOT call clearPersistedState() - keep sessionStorage so summary survives page refresh
        // Show reconcile prompt after successful import
        setShowReconcilePrompt(true);
      }
    } catch (error) {
      setBankImportResult({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error'
      });
    } finally {
      setIsImporting(false);
    }
  };

  // Scan emails for bank statements
  const handleScanEmails = async () => {
    setEmailScanLoading(true);
    setEmailStatements([]);

    try {
      // Use appropriate endpoint based on data source
      let url: string;
      if (dataSource === 'opera3') {
        if (!opera3DataPath) {
          setEmailScanLoading(false);
          return;
        }
        url = `${API_BASE}/opera3/bank-import/scan-emails?bank_code=${selectedBankCode}&data_path=${encodeURIComponent(opera3DataPath)}&days_back=${emailScanDaysBack}&include_processed=false`;
      } else {
        url = `${API_BASE}/bank-import/scan-emails?bank_code=${selectedBankCode}&days_back=${emailScanDaysBack}&include_processed=false`;
      }

      const response = await authFetch(url);
      const data = await response.json();

      if (data.success) {
        setEmailStatements(data.statements_found || []);
      }
    } catch (error) {
      console.error('Error scanning emails:', error);
    } finally {
      setEmailScanLoading(false);
    }
  };

  // Preview bank statement from email attachment
  const handleEmailPreview = async (emailId: number, attachmentId: string, filename: string) => {
    setIsPreviewing(true);
    setBankPreview(null);
    setRawFilePreview(null);
    setShowRawPreview(false);
    setBankImportResult(null);
    setEditedTransactions(new Map());
    setIncludedSkipped(new Map());
    setTransactionTypeOverrides(new Map());
    setRefundOverrides(new Map());
    setTabSearchFilter('');
    setSelectedEmailStatement({ emailId, attachmentId, filename });

    try {
      // Use appropriate endpoint based on data source
      let url: string;
      if (dataSource === 'opera3') {
        if (!opera3DataPath) {
          setBankPreview({
            success: false,
            filename: filename,
            total_transactions: 0,
            matched_receipts: [],
            matched_payments: [],
            matched_refunds: [],
            repeat_entries: [],
            unmatched: [],
            already_posted: [],
            skipped: [],
            errors: ['Opera 3 data path is required. Please configure it above.']
          });
          setIsPreviewing(false);
          return;
        }
        url = `${API_BASE}/opera3/bank-import/preview-from-email?email_id=${emailId}&attachment_id=${encodeURIComponent(attachmentId)}&data_path=${encodeURIComponent(opera3DataPath)}&bank_code=${selectedBankCode}`;
      } else {
        url = `${API_BASE}/bank-import/preview-from-email?email_id=${emailId}&attachment_id=${encodeURIComponent(attachmentId)}&bank_code=${selectedBankCode}`;
      }
      const response = await authFetch(url, { method: 'POST' });
      const data = await response.json();

      if (data.bank_mismatch) {
        setBankPreview({
          success: false,
          filename: filename,
          total_transactions: 0,
          matched_receipts: [],
          matched_payments: [],
          matched_refunds: [],
          repeat_entries: [],
          unmatched: [],
          already_posted: [],
          skipped: [],
          errors: [
            `Bank account mismatch: The statement is for bank ${data.detected_bank}, but you selected ${data.selected_bank}.`,
            'Please select the correct bank account and try again.'
          ]
        });
        return;
      }

      // Handle statement sequence validation responses
      if (data.status === 'skipped') {
        // This statement has already been processed (opening balance < reconciled balance)
        // The backend has auto-marked it as processed - show detailed error from backend
        setBankPreview({
          success: false,
          filename: filename,
          total_transactions: 0,
          matched_receipts: [],
          matched_payments: [],
          matched_refunds: [],
          repeat_entries: [],
          unmatched: [],
          already_posted: [],
          skipped: [],
          statement_bank_info: data.statement_info ? {
            bank_name: data.statement_info.bank_name,
            account_number: data.statement_info.account_number,
            opening_balance: data.statement_info.opening_balance,
            closing_balance: data.statement_info.closing_balance,
            statement_date: data.statement_info.period_end
          } : undefined,
          errors: data.errors || [
            `Statement already processed or superseded.`,
            `The statement opening balance (£${data.statement_info?.opening_balance?.toFixed(2) || '?'}) is less than Opera's reconciled balance (£${data.reconciled_balance?.toFixed(2) || '?'}).`
          ]
        });
        // Refresh the email statements list to remove this statement
        setEmailStatements(prev => prev.filter(e => e.email_id !== emailId));
        setSelectedEmailStatement(null);
        return;
      }

      if (data.status === 'pending') {
        // Future statement - missing one in between
        setBankPreview({
          success: false,
          filename: filename,
          total_transactions: 0,
          matched_receipts: [],
          matched_payments: [],
          matched_refunds: [],
          repeat_entries: [],
          unmatched: [],
          already_posted: [],
          skipped: [],
          errors: [
            `Statement out of sequence - missing earlier statement.`,
            `Statement opening balance: £${data.statement_info?.opening_balance?.toFixed(2) || '?'}`,
            `Opera reconciled balance: £${data.reconciled_balance?.toFixed(2) || '?'}`,
            `Please import the missing statement(s) first, or manually reconcile to £${data.statement_info?.opening_balance?.toFixed(2) || '?'} in Opera.`
          ]
        });
        return;
      }

      // Determine default format based on file extension if backend doesn't specify
      const isEmailPdf = filename.toLowerCase().endsWith('.pdf');
      const emailDefaultFormat = isEmailPdf ? 'PDF' : 'CSV';
      const enhancedPreview: EnhancedBankImportPreview = {
        success: data.success,
        filename: data.filename,
        detected_format: data.detected_format || emailDefaultFormat,
        total_transactions: data.total_transactions,
        matched_receipts: data.matched_receipts || [],
        matched_payments: data.matched_payments || [],
        matched_refunds: data.matched_refunds || [],
        repeat_entries: data.repeat_entries || [],
        unmatched: data.unmatched || [],
        already_posted: data.already_posted || [],
        skipped: data.skipped || [],
        summary: data.summary,
        errors: data.errors || (data.error ? [data.error] : []),
        // Include statement bank info from AI extraction (for PDF statements)
        statement_bank_info: data.statement_bank_info ? {
          bank_name: data.statement_bank_info.bank_name,
          account_number: data.statement_bank_info.account_number,
          sort_code: data.statement_bank_info.sort_code,
          statement_date: data.statement_bank_info.statement_date,
          opening_balance: data.statement_bank_info.opening_balance,
          closing_balance: data.statement_bank_info.closing_balance,
          matched_opera_bank: data.statement_bank_info.matched_opera_bank,
          matched_opera_name: data.statement_bank_info.matched_opera_name
        } : undefined
      };

      setBankPreview(enhancedPreview);

      // Initialize selectedForImport
      const preSelected = new Set<number>();
      enhancedPreview.matched_receipts.filter(t => !t.is_duplicate).forEach(t => preSelected.add(t.row));
      enhancedPreview.matched_payments.filter(t => !t.is_duplicate).forEach(t => preSelected.add(t.row));
      (enhancedPreview.matched_refunds || []).filter(t => !t.is_duplicate).forEach(t => preSelected.add(t.row));
      setSelectedForImport(preSelected);

      setDateOverrides(new Map());
      setUpdatedRepeatEntries(new Set());

      if (enhancedPreview.matched_receipts.length > 0) setActivePreviewTab('receipts');
      else if (enhancedPreview.matched_payments.length > 0) setActivePreviewTab('payments');
      else if (enhancedPreview.matched_refunds?.length > 0) setActivePreviewTab('refunds');
      else if (enhancedPreview.repeat_entries?.length > 0) setActivePreviewTab('repeat');
      else if (enhancedPreview.unmatched.length > 0) setActivePreviewTab('unmatched');
      else setActivePreviewTab('skipped');
    } catch (error) {
      setBankPreview({
        success: false,
        filename: filename,
        total_transactions: 0,
        matched_receipts: [],
        matched_payments: [],
        matched_refunds: [],
        repeat_entries: [],
        unmatched: [],
        already_posted: [],
        skipped: [],
        errors: [error instanceof Error ? error.message : 'Unknown error']
      });
    } finally {
      setIsPreviewing(false);
    }
  };

  // View PDF file from filesystem in popup
  const handlePdfFileView = (filename: string) => {
    if (!pdfDirectory || !filename) return;

    const fullPath = pdfDirectory.endsWith('/') || pdfDirectory.endsWith('\\')
      ? pdfDirectory + filename
      : pdfDirectory + '/' + filename;

    // Open PDF directly using the file view API
    const viewUrl = `${API_BASE}/file/view?path=${encodeURIComponent(fullPath)}`;
    setPdfViewerData({ data: '', filename, viewUrl });
  };

  // Preview bank statement from PDF file (similar to email preview)
  const handlePdfPreview = async (filename: string) => {
    if (!pdfDirectory || !filename) return;

    const fullPath = pdfDirectory.endsWith('/') || pdfDirectory.endsWith('\\')
      ? pdfDirectory + filename
      : pdfDirectory + '/' + filename;

    setIsPreviewing(true);
    setBankPreview(null);
    setRawFilePreview(null);
    setShowRawPreview(false);
    setBankImportResult(null);
    setEditedTransactions(new Map());
    setIncludedSkipped(new Map());
    setTransactionTypeOverrides(new Map());
    setRefundOverrides(new Map());
    setTabSearchFilter('');
    setSelectedPdfFile({ filename, fullPath });
    setSelectedEmailStatement(null);

    try {
      // Use appropriate endpoint based on data source
      let url: string;
      if (dataSource === 'opera3') {
        if (!opera3DataPath) {
          setBankPreview({
            success: false,
            filename: filename,
            total_transactions: 0,
            matched_receipts: [],
            matched_payments: [],
            matched_refunds: [],
            repeat_entries: [],
            unmatched: [],
            already_posted: [],
            skipped: [],
            errors: ['Opera 3 data path is required. Please configure it in Settings.']
          });
          setIsPreviewing(false);
          return;
        }
        url = `${API_BASE}/opera3/bank-import/preview-from-pdf?file_path=${encodeURIComponent(fullPath)}&data_path=${encodeURIComponent(opera3DataPath)}&bank_code=${selectedBankCode}`;
      } else {
        url = `${API_BASE}/bank-import/preview-from-pdf?file_path=${encodeURIComponent(fullPath)}&bank_code=${selectedBankCode}`;
      }
      const response = await authFetch(url, { method: 'POST' });
      const data = await response.json();

      if (data.bank_mismatch) {
        setBankPreview({
          success: false,
          filename: filename,
          total_transactions: 0,
          matched_receipts: [],
          matched_payments: [],
          matched_refunds: [],
          repeat_entries: [],
          unmatched: [],
          already_posted: [],
          skipped: [],
          errors: [
            `Bank account mismatch: The statement is for bank ${data.detected_bank}, but you selected ${data.selected_bank}.`,
            'Please select the correct bank account and try again.'
          ]
        });
        return;
      }

      // Handle statement sequence validation responses
      if (data.status === 'skipped') {
        setBankPreview({
          success: false,
          filename: filename,
          total_transactions: 0,
          matched_receipts: [],
          matched_payments: [],
          matched_refunds: [],
          repeat_entries: [],
          unmatched: [],
          already_posted: [],
          skipped: [],
          statement_bank_info: data.statement_info ? {
            bank_name: data.statement_info.bank_name,
            account_number: data.statement_info.account_number,
            opening_balance: data.statement_info.opening_balance,
            closing_balance: data.statement_info.closing_balance,
            statement_date: data.statement_info.statement_date
          } : undefined,
          errors: [data.message || 'This statement appears to have already been processed.']
        });
        return;
      }

      if (data.status === 'out_of_sequence') {
        setBankPreview({
          success: false,
          filename: filename,
          total_transactions: 0,
          matched_receipts: [],
          matched_payments: [],
          matched_refunds: [],
          repeat_entries: [],
          unmatched: [],
          already_posted: [],
          skipped: [],
          statement_bank_info: data.statement_info ? {
            bank_name: data.statement_info.bank_name,
            account_number: data.statement_info.account_number,
            opening_balance: data.statement_info.opening_balance,
            closing_balance: data.statement_info.closing_balance,
            statement_date: data.statement_info.statement_date
          } : undefined,
          errors: [
            data.message || 'Statement is out of sequence.',
            `Opening balance: £${data.statement_info?.opening_balance?.toLocaleString('en-GB', { minimumFractionDigits: 2 })}`,
            `Reconciled balance: £${data.reconciled_balance?.toLocaleString('en-GB', { minimumFractionDigits: 2 })}`
          ]
        });
        return;
      }

      // Success - build preview
      const enhancedPreview: EnhancedBankImportPreview = {
        success: data.success,
        filename: filename,
        detected_format: data.detected_format || 'PDF',
        total_transactions: data.total_transactions || 0,
        matched_receipts: data.matched_receipts || [],
        matched_payments: data.matched_payments || [],
        matched_refunds: data.matched_refunds || [],
        repeat_entries: data.repeat_entries || [],
        unmatched: data.unmatched || [],
        already_posted: data.already_posted || [],
        skipped: data.skipped || [],
        summary: data.summary,
        errors: data.errors || [],
        period_info: data.period_info,
        period_violations: data.period_violations,
        has_period_violations: data.has_period_violations,
        statement_bank_info: data.statement_bank_info ? {
          bank_name: data.statement_bank_info.bank_name,
          account_number: data.statement_bank_info.account_number,
          sort_code: data.statement_bank_info.sort_code,
          statement_date: data.statement_bank_info.statement_date,
          opening_balance: data.statement_bank_info.opening_balance,
          closing_balance: data.statement_bank_info.closing_balance,
          matched_opera_bank: data.statement_bank_info.matched_opera_bank,
          matched_opera_name: data.statement_bank_info.matched_opera_name
        } : undefined
      };

      setBankPreview(enhancedPreview);

      // Initialize selectedForImport
      const preSelected = new Set<number>();
      enhancedPreview.matched_receipts.filter(t => !t.is_duplicate).forEach(t => preSelected.add(t.row));
      enhancedPreview.matched_payments.filter(t => !t.is_duplicate).forEach(t => preSelected.add(t.row));
      (enhancedPreview.matched_refunds || []).filter(t => !t.is_duplicate).forEach(t => preSelected.add(t.row));
      setSelectedForImport(preSelected);

      setDateOverrides(new Map());
      setUpdatedRepeatEntries(new Set());

      if (enhancedPreview.matched_receipts.length > 0) setActivePreviewTab('receipts');
      else if (enhancedPreview.matched_payments.length > 0) setActivePreviewTab('payments');
      else if (enhancedPreview.matched_refunds?.length > 0) setActivePreviewTab('refunds');
      else if (enhancedPreview.repeat_entries?.length > 0) setActivePreviewTab('repeat');
      else if (enhancedPreview.unmatched.length > 0) setActivePreviewTab('unmatched');
      else setActivePreviewTab('skipped');
    } catch (error) {
      setBankPreview({
        success: false,
        filename: filename,
        total_transactions: 0,
        matched_receipts: [],
        matched_payments: [],
        matched_refunds: [],
        repeat_entries: [],
        unmatched: [],
        already_posted: [],
        skipped: [],
        errors: [error instanceof Error ? error.message : 'Unknown error']
      });
    } finally {
      setIsPreviewing(false);
    }
  };

  // Import bank statement from PDF file
  const handlePdfImport = async () => {
    if (!selectedPdfFile) {
      console.error('handlePdfImport called but selectedPdfFile is null');
      setBankImportResult({
        success: false,
        error: 'No PDF file selected. Please select a PDF file and run preview first.'
      });
      return;
    }

    setIsImporting(true);
    setBankImportResult(null);

    try {
      // Prepare overrides - include transactions with account OR those that don't need account (nominal/bank transfer)
      const unmatchedOverrides = Array.from(selectedForImport).map(row => {
        const editedTxn = editedTransactions.get(row);
        const txnType = transactionTypeOverrides.get(row);
        const isNlOrTransfer = txnType === 'bank_transfer' || txnType === 'nominal_receipt' || txnType === 'nominal_payment';
        if (editedTxn?.manual_account || isNlOrTransfer) {
          return {
            row,
            account: editedTxn?.manual_account || '',
            ledger_type: editedTxn?.manual_ledger_type || 'C',
            transaction_type: txnType || (editedTxn?.manual_ledger_type === 'C' ? 'sales_receipt' : 'purchase_payment')
          };
        }
        return null;
      }).filter(Boolean);

      const skippedOverrides = Array.from(includedSkipped.entries())
        .filter(([, data]) => {
          const isNlOrTransfer = data.transaction_type === 'bank_transfer' || data.transaction_type === 'nominal_receipt' || data.transaction_type === 'nominal_payment';
          return data.account || isNlOrTransfer;
        })
        .map(([row, data]) => ({
          row,
          account: data.account || '',
          ledger_type: data.ledger_type,
          transaction_type: data.transaction_type
        }));

      const refundOverridesList = Array.from(refundOverrides.entries())
        .filter(([row, data]) => selectedForImport.has(row) && (data.transaction_type || data.account))
        .map(([row, data]) => ({
          row,
          account: data.account,
          ledger_type: data.ledger_type,
          transaction_type: data.transaction_type
        }));

      const allOverrides = [...unmatchedOverrides, ...skippedOverrides, ...refundOverridesList];
      const selectedRowsList = Array.from(selectedForImport);
      const dateOverridesList = Array.from(dateOverrides.entries()).map(([row, date]) => ({ row, date }));
      const rejectedRefundRows = Array.from(refundOverrides.entries())
        .filter(([row, data]) => data.rejected && !selectedForImport.has(row))
        .map(([row]) => row);

      // Include per-row auto-allocate disabled flags
      const autoAllocateDisabledRows = Array.from(autoAllocateDisabled).filter(row => selectedRowsList.includes(row));

      let url: string;
      if (dataSource === 'opera3') {
        url = `${API_BASE}/opera3/bank-import/import-from-pdf?file_path=${encodeURIComponent(selectedPdfFile.fullPath)}&data_path=${encodeURIComponent(opera3DataPath)}&bank_code=${selectedBankCode}&auto_allocate=${autoAllocate}`;
      } else {
        url = `${API_BASE}/bank-import/import-from-pdf?file_path=${encodeURIComponent(selectedPdfFile.fullPath)}&bank_code=${selectedBankCode}&auto_allocate=${autoAllocate}`;
      }

      const response = await authFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          overrides: allOverrides,
          selected_rows: selectedRowsList,
          date_overrides: dateOverridesList,
          rejected_refund_rows: rejectedRefundRows,
          auto_allocate_disabled_rows: autoAllocateDisabledRows  // Rows to skip auto-allocation
        })
      });

      const data = await response.json();
      setBankImportResult(data);

      if (data.success) {
        setShowReconcilePrompt(true);
        // Refresh PDF list to show as processed
        handleScanPdfFiles();
      }
    } catch (error) {
      setBankImportResult({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error'
      });
    } finally {
      setIsImporting(false);
    }
  };

  // Import bank statement from email attachment
  const handleEmailImport = async () => {
    if (!selectedEmailStatement) {
      console.error('handleEmailImport called but selectedEmailStatement is null');
      setBankImportResult({
        success: false,
        error: 'No email statement selected. Please select an email attachment and run preview first.'
      });
      return;
    }

    setIsImporting(true);
    setBankImportResult(null);

    try {
      // Prepare overrides - include transactions with account OR those that don't need account (nominal/bank transfer)
      const unmatchedOverrides = Array.from(selectedForImport).map(row => {
        const editedTxn = editedTransactions.get(row);
        const txnType = transactionTypeOverrides.get(row);
        const isNlOrTransfer = txnType === 'bank_transfer' || txnType === 'nominal_receipt' || txnType === 'nominal_payment';
        if (editedTxn?.manual_account || isNlOrTransfer) {
          return {
            row,
            account: editedTxn?.manual_account || '',
            ledger_type: editedTxn?.manual_ledger_type || 'C',
            transaction_type: txnType || (editedTxn?.manual_ledger_type === 'C' ? 'sales_receipt' : 'purchase_payment')
          };
        }
        return null;
      }).filter(Boolean);

      const skippedOverrides = Array.from(includedSkipped.entries())
        .filter(([, data]) => {
          const isNlOrTransfer = data.transaction_type === 'bank_transfer' || data.transaction_type === 'nominal_receipt' || data.transaction_type === 'nominal_payment';
          return data.account || isNlOrTransfer;
        })
        .map(([row, data]) => ({
          row,
          account: data.account || '',
          ledger_type: data.ledger_type,
          transaction_type: data.transaction_type
        }));

      const refundOverridesList = Array.from(refundOverrides.entries())
        .filter(([row, data]) => selectedForImport.has(row) && (data.transaction_type || data.account))
        .map(([row, data]) => ({
          row,
          account: data.account,
          ledger_type: data.ledger_type,
          transaction_type: data.transaction_type
        }));

      const overrides = [...unmatchedOverrides, ...skippedOverrides, ...refundOverridesList];
      const selectedRowsArray = Array.from(selectedForImport);
      const dateOverridesList = Array.from(dateOverrides.entries()).map(([row, date]) => ({
        row,
        date
      }));

      // Include per-row auto-allocate disabled flags
      const autoAllocateDisabledRows = Array.from(autoAllocateDisabled).filter(row => selectedRowsArray.includes(row));

      const url = `${API_BASE}/bank-import/import-from-email?email_id=${selectedEmailStatement.emailId}&attachment_id=${encodeURIComponent(selectedEmailStatement.attachmentId)}&bank_code=${selectedBankCode}&auto_allocate=${autoAllocate}`;
      const response = await authFetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          overrides,
          selected_rows: selectedRowsArray,
          date_overrides: dateOverridesList,
          auto_allocate_disabled_rows: autoAllocateDisabledRows  // Rows to skip auto-allocation
        })
      });
      const data = await response.json();
      setBankImportResult(data);

      if (data.success) {
        setEditedTransactions(new Map());
        setIncludedSkipped(new Map());
        setTransactionTypeOverrides(new Map());
        setRefundOverrides(new Map());
        setSelectedForImport(new Set());
        setDateOverrides(new Map());
        setAutoAllocateDisabled(new Set());
        // Note: Do NOT clear bankPreview or sessionStorage - keep summary visible until user clicks "Clear Statement"
        // Refresh email list to show updated processed state
        handleScanEmails();
        // Show reconcile prompt after successful import
        setShowReconcilePrompt(true);
      }
    } catch (error) {
      setBankImportResult({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error'
      });
    } finally {
      setIsImporting(false);
    }
  };

  const handleImport = async () => {
    setLoading(true);
    setResult(null);

    try {
      let endpoint = '';
      let body: any = {};

      switch (activeType) {
        case 'sales-receipt':
          endpoint = '/opera-sql/sales-receipt';
          body = {
            bank_account: bankAccount,
            customer_account: customerAccount,
            amount: parseFloat(receiptAmount),
            reference: reference,
            post_date: postDate,
            input_by: inputBy,
            validate_only: validateOnly
          };
          break;

        case 'purchase-payment':
          endpoint = '/opera-sql/purchase-payment';
          body = {
            bank_account: bankAccount,
            supplier_account: supplierAccount,
            amount: parseFloat(paymentAmount),
            reference: reference,
            post_date: postDate,
            input_by: inputBy,
            validate_only: validateOnly
          };
          break;

        case 'sales-invoice':
          endpoint = '/opera-sql/sales-invoice';
          body = {
            customer_account: customerAccount,
            invoice_number: invoiceNumber,
            net_amount: parseFloat(netAmount),
            vat_amount: parseFloat(vatAmount || '0'),
            post_date: postDate,
            nominal_account: nominalAccount || 'GA010',
            input_by: inputBy,
            description: description,
            validate_only: validateOnly
          };
          break;

        case 'purchase-invoice':
          endpoint = '/opera-sql/purchase-invoice';
          body = {
            supplier_account: supplierAccount,
            invoice_number: invoiceNumber,
            net_amount: parseFloat(netAmount),
            vat_amount: parseFloat(vatAmount || '0'),
            post_date: postDate,
            nominal_account: nominalAccount || 'HA010',
            input_by: inputBy,
            description: description,
            validate_only: validateOnly
          };
          break;

        case 'nominal-journal':
          endpoint = '/opera-sql/nominal-journal';
          body = {
            lines: journalLines
              .filter(l => l.account && l.amount)
              .map(l => ({
                account: l.account,
                amount: parseFloat(l.amount),
                description: l.description
              })),
            reference: reference,
            post_date: postDate,
            input_by: inputBy,
            description: description,
            validate_only: validateOnly
          };
          break;
      }

      const response = await authFetch(`${API_BASE}${endpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });

      const data = await response.json();
      setResult(data);
    } catch (error) {
      setResult({
        success: false,
        validate_only: validateOnly,
        records_processed: 0,
        records_imported: 0,
        records_failed: 1,
        errors: [error instanceof Error ? error.message : 'Unknown error'],
        details: []
      });
    } finally {
      setLoading(false);
    }
  };

  const addJournalLine = () => {
    setJournalLines([...journalLines, { account: '', amount: '', description: '' }]);
  };

  const updateJournalLine = (index: number, field: string, value: string) => {
    const newLines = [...journalLines];
    newLines[index] = { ...newLines[index], [field]: value };
    setJournalLines(newLines);
  };

  const removeJournalLine = (index: number) => {
    if (journalLines.length > 2) {
      setJournalLines(journalLines.filter((_, i) => i !== index));
    }
  };

  const journalTotal = journalLines.reduce((sum, l) => sum + (parseFloat(l.amount) || 0), 0);

  const importTypes = [
    { id: 'bank-statement' as ImportType, label: 'Bank Statement Import', icon: Landmark, color: 'emerald' },
    { id: 'sales-receipt' as ImportType, label: 'Sales Receipt', icon: Receipt, color: 'green' },
    { id: 'purchase-payment' as ImportType, label: 'Purchase Payment', icon: CreditCard, color: 'red' },
    { id: 'sales-invoice' as ImportType, label: 'Sales Invoice', icon: FileText, color: 'blue' },
    { id: 'purchase-invoice' as ImportType, label: 'Purchase Invoice', icon: FileSpreadsheet, color: 'orange' },
    { id: 'nominal-journal' as ImportType, label: 'Nominal Journal', icon: BookOpen, color: 'purple' }
  ];

  // Handle ignoring a transaction (mark it so it won't appear in future reconciliations)
  const handleIgnoreTransaction = async () => {
    if (!ignoreConfirm || !selectedBankCode) {
      alert('Missing bank code or transaction details');
      return;
    }

    setIsIgnoring(true);
    try {
      const params = new URLSearchParams();
      params.append('transaction_date', ignoreConfirm.date);
      params.append('amount', ignoreConfirm.amount.toString());
      if (ignoreConfirm.description) {
        params.append('description', ignoreConfirm.description);
      }
      params.append('reason', 'Already entered in Opera');

      const url = `${API_BASE}/reconcile/bank/${encodeURIComponent(selectedBankCode)}/ignore-transaction?${params.toString()}`;
      console.log('Ignore transaction URL:', url);

      const response = await authFetch(url, { method: 'POST' });
      const data = await response.json();

      if (data.success) {
        // Mark this row as ignored
        setIgnoredTransactions(prev => new Set([...prev, ignoreConfirm.row]));
        // Also deselect it from import
        setSelectedForImport(prev => {
          const newSet = new Set(prev);
          newSet.delete(ignoreConfirm.row);
          return newSet;
        });
        setIgnoreConfirm(null);
      } else {
        alert(`Error: ${data.error || data.detail || 'Unknown error'}`);
      }
    } catch (error) {
      console.error('Ignore transaction error:', error);
      alert(`Failed to ignore transaction: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      setIsIgnoring(false);
    }
  };

  // Open ignore confirmation modal
  const openIgnoreConfirm = (txn: BankImportTransaction) => {
    // Extract just the date part (YYYY-MM-DD) if it contains a timestamp
    const dateOnly = txn.date.includes('T') ? txn.date.split('T')[0] : txn.date;
    // Clean description - remove newlines
    const cleanDescription = (txn.name || txn.reference || '').replace(/[\n\r]/g, ' ').trim();
    setIgnoreConfirm({
      row: txn.row,
      date: dateOnly,
      description: cleanDescription,
      amount: txn.amount
    });
  };

  // Open nominal detail modal when selecting nominal type
  const openNominalDetailModal = (txn: BankImportTransaction, txnType: TransactionType, source: 'unmatched' | 'refund' | 'skipped') => {
    // Initialize form state from existing detail or defaults
    const existingDetail = nominalPostingDetails.get(txn.row);
    const grossAmount = Math.abs(txn.amount);
    setModalNominalCode(existingDetail?.nominalCode || '');
    // Initialize search with existing nominal display if editing
    const existingNominal = existingDetail?.nominalCode
      ? nominalAccounts.find(n => n.code === existingDetail.nominalCode)
      : null;
    setModalNominalSearch(existingNominal ? `${existingNominal.code} - ${existingNominal.description}` : '');
    setModalNominalDropdownOpen(false);
    // Default VAT code to N/A for nominal postings
    setModalVatCode(existingDetail?.vatCode || 'N/A');
    // Initialize VAT search
    const existingVat = existingDetail?.vatCode && existingDetail.vatCode !== 'N/A'
      ? vatCodes.find(v => v.code === existingDetail.vatCode)
      : null;
    setModalVatSearch(existingDetail?.vatCode && existingDetail.vatCode !== 'N/A' && existingVat
      ? `${existingVat.code} - ${existingVat.description} (${existingVat.rate}%)`
      : 'N/A');
    setModalVatDropdownOpen(false);
    setModalNetAmount(existingDetail?.netAmount?.toString() || grossAmount.toFixed(2));
    setModalVatAmount(existingDetail?.vatAmount?.toString() || '0.00');

    setNominalDetailModal({
      open: true,
      transaction: txn,
      transactionType: txnType,
      source
    });
  };

  // Handle saving nominal detail from modal
  const handleSaveNominalDetail = (detail: NominalPostingDetail) => {
    if (!nominalDetailModal.transaction) return;

    const row = nominalDetailModal.transaction.row;
    const txn = nominalDetailModal.transaction;
    const source = nominalDetailModal.source;
    const txnType = nominalDetailModal.transactionType;

    // Save the nominal detail
    setNominalPostingDetails(prev => {
      const updated = new Map(prev);
      updated.set(row, detail);
      return updated;
    });

    // Also update the edited transaction with the nominal account
    if (source === 'unmatched') {
      const updated = new Map(editedTransactions);
      updated.set(row, {
        ...txn,
        manual_account: detail.nominalCode,
        manual_ledger_type: 'S' as const, // N for nominal, but type doesn't have N
        account_name: detail.nominalDescription || '',
        isEdited: true,
        nominal_detail: detail
      });
      setEditedTransactions(updated);

      // Set transaction type override
      if (txnType) {
        setTransactionTypeOverrides(prev => {
          const updated = new Map(prev);
          updated.set(row, txnType);
          return updated;
        });
      }

      // Auto-select for import
      setSelectedForImport(prev => new Set(prev).add(row));
    } else if (source === 'refund') {
      // Update refund overrides
      setRefundOverrides(prev => {
        const updated = new Map(prev);
        const current = updated.get(row) || {};
        updated.set(row, {
          ...current,
          transaction_type: txnType || undefined,
          account: detail.nominalCode,
          ledger_type: 'S' as const
        });
        return updated;
      });
      setSelectedForImport(prev => new Set(prev).add(row));
    } else if (source === 'skipped') {
      // Update included skipped
      setIncludedSkipped(prev => {
        const updated = new Map(prev);
        updated.set(row, {
          account: detail.nominalCode,
          ledger_type: 'S' as const,
          transaction_type: txnType || 'nominal_receipt'
        });
        return updated;
      });
      setSelectedForImport(prev => new Set(prev).add(row));
    }

    // Close modal
    setNominalDetailModal({ open: false, transaction: null, transactionType: null, source: 'unmatched' });
  };

  // Open bank transfer modal
  const openBankTransferModal = (txn: BankImportTransaction, source: 'unmatched' | 'refund' | 'skipped') => {
    // Initialize form state from existing detail or defaults from transaction
    const existingDetail = bankTransferDetails.get(txn.row);
    setModalDestBank(existingDetail?.destBankCode || '');
    // Initialize bank search
    const existingBank = existingDetail?.destBankCode
      ? bankAccounts.find(b => b.code === existingDetail.destBankCode)
      : null;
    setModalDestBankSearch(existingBank ? `${existingBank.code} - ${existingBank.description}` : '');
    setModalDestBankDropdownOpen(false);
    setModalCashbookType(existingDetail?.cashbookType || 'TRF');
    setModalReference(existingDetail?.reference || txn.name?.substring(0, 20) || '');
    setModalComment(existingDetail?.comment || txn.name || '');
    setModalDate(existingDetail?.date || txn.date || '');

    setBankTransferModal({ open: true, transaction: txn, source });
  };

  // Handle saving bank transfer detail
  const handleSaveBankTransfer = () => {
    if (!bankTransferModal.transaction) return;

    const row = bankTransferModal.transaction.row;
    const txn = bankTransferModal.transaction;
    const source = bankTransferModal.source;
    const destBankCode = modalDestBank;
    const destBankName = bankAccounts.find(b => b.code === modalDestBank)?.description || '';

    // Save the bank transfer detail with all fields
    setBankTransferDetails(prev => {
      const updated = new Map(prev);
      updated.set(row, {
        destBankCode,
        destBankName,
        cashbookType: modalCashbookType,
        reference: modalReference,
        comment: modalComment,
        date: modalDate
      });
      return updated;
    });

    // Update the appropriate state based on source
    if (source === 'unmatched') {
      const updated = new Map(editedTransactions);
      updated.set(row, {
        ...txn,
        manual_account: destBankCode,
        manual_ledger_type: 'S' as const,
        account_name: destBankName,
        isEdited: true
      });
      setEditedTransactions(updated);

      // Set transaction type override
      setTransactionTypeOverrides(prev => {
        const updated = new Map(prev);
        updated.set(row, 'bank_transfer');
        return updated;
      });

      setSelectedForImport(prev => new Set(prev).add(row));
    } else if (source === 'refund') {
      setRefundOverrides(prev => {
        const updated = new Map(prev);
        const current = updated.get(row) || {};
        updated.set(row, {
          ...current,
          transaction_type: 'bank_transfer',
          account: destBankCode,
          ledger_type: 'S' as const
        });
        return updated;
      });
      setSelectedForImport(prev => new Set(prev).add(row));
    } else if (source === 'skipped') {
      setIncludedSkipped(prev => {
        const updated = new Map(prev);
        updated.set(row, {
          account: destBankCode,
          ledger_type: 'S' as const,
          transaction_type: 'bank_transfer'
        });
        return updated;
      });
      setSelectedForImport(prev => new Set(prev).add(row));
    }

    setBankTransferModal({ open: false, transaction: null, source: 'unmatched' });
  };

  // Render Bank Transfer Modal
  const renderBankTransferModal = () => {
    if (!bankTransferModal.open || !bankTransferModal.transaction) return null;

    const txn = bankTransferModal.transaction;
    const amount = txn.amount;
    const isOutgoing = amount < 0;

    // Use component-level state (initialized in openBankTransferModal)
    const selectedDestBank = bankAccounts.find(b => b.code === modalDestBank);
    const canSave = !!modalDestBank && !!modalReference && !!modalDate;

    return (
      <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
        <div className="bg-white rounded-lg shadow-xl w-full max-w-md mx-4">
          {/* Header */}
          <div className={`px-6 py-4 border-b ${isOutgoing ? 'bg-red-50 border-red-200' : 'bg-green-50 border-green-200'}`}>
            <div className="flex justify-between items-center">
              <h3 className={`text-lg font-semibold ${isOutgoing ? 'text-red-800' : 'text-green-800'}`}>
                Bank Transfer {isOutgoing ? 'Out' : 'In'}
              </h3>
              <button
                onClick={() => setBankTransferModal({ open: false, transaction: null, source: 'unmatched' })}
                className="text-gray-400 hover:text-gray-600"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <div className="mt-2 text-sm text-gray-600">
              <div className="flex justify-between">
                <span>{txn.name}</span>
                <span className={`font-medium ${isOutgoing ? 'text-red-700' : 'text-green-700'}`}>
                  {isOutgoing ? '-' : '+'}£{Math.abs(amount).toFixed(2)}
                </span>
              </div>
              <div className="text-xs text-gray-500 mt-1">{txn.date}</div>
            </div>
          </div>

          {/* Form */}
          <div className="px-6 py-4 space-y-4">
            {/* Transfer direction explanation */}
            <div className={`p-3 rounded ${isOutgoing ? 'bg-red-50' : 'bg-green-50'}`}>
              <div className="flex items-center gap-2 text-sm">
                <Landmark className={`h-4 w-4 ${isOutgoing ? 'text-red-600' : 'text-green-600'}`} />
                <span className={isOutgoing ? 'text-red-700' : 'text-green-700'}>
                  {isOutgoing
                    ? `Transferring FROM ${selectedBankCode} TO another bank`
                    : `Transferring INTO ${selectedBankCode} FROM another bank`
                  }
                </span>
              </div>
            </div>

            {/* Header fields row */}
            <div className="grid grid-cols-2 gap-4">
              {/* Cashbook Type */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Cashbook Type <span className="text-red-500">*</span>
                </label>
                <select
                  value={modalCashbookType}
                  onChange={(e) => setModalCashbookType(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="TRF">TRF - Transfer</option>
                  <option value="CHQ">CHQ - Cheque</option>
                  <option value="CSH">CSH - Cash</option>
                  <option value="DDR">DDR - Direct Debit</option>
                  <option value="BGC">BGC - Bank Giro Credit</option>
                  <option value="STO">STO - Standing Order</option>
                </select>
              </div>

              {/* Date */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Date <span className="text-red-500">*</span>
                </label>
                <input
                  type="date"
                  value={modalDate}
                  onChange={(e) => setModalDate(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
            </div>

            {/* Reference */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Reference <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={modalReference}
                onChange={(e) => setModalReference(e.target.value)}
                maxLength={20}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="Max 20 characters"
              />
            </div>

            {/* Comment */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Comment
              </label>
              <input
                type="text"
                value={modalComment}
                onChange={(e) => setModalComment(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                placeholder="Description/memo"
              />
            </div>

            {/* Destination/Source Bank - Searchable */}
            {(() => {
              const filteredDestBanks = bankAccounts
                .filter(b => b.code !== selectedBankCode)
                .filter(b => {
                  if (!modalDestBankSearch) return true;
                  const search = modalDestBankSearch.toLowerCase();
                  return b.code.toLowerCase().includes(search) ||
                         b.description.toLowerCase().includes(search) ||
                         (b.sort_code && b.sort_code.includes(search)) ||
                         (b.account_number && b.account_number.includes(search));
                });
              return (
            <div className="relative">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                {isOutgoing ? 'Destination Bank' : 'Source Bank'} <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={modalDestBankSearch}
                onChange={(e) => {
                  setModalDestBankSearch(e.target.value);
                  setModalDestBankDropdownOpen(true);
                  setModalDestBankHighlightIndex(0);
                  // Clear selection if user edits
                  if (modalDestBank) {
                    setModalDestBank('');
                  }
                }}
                onFocus={() => {
                  setModalDestBankDropdownOpen(true);
                  setModalDestBankHighlightIndex(0);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    if (!modalDestBankDropdownOpen) {
                      setModalDestBankDropdownOpen(true);
                    } else {
                      setModalDestBankHighlightIndex(prev => Math.min(prev + 1, filteredDestBanks.length - 1));
                    }
                  } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    setModalDestBankHighlightIndex(prev => Math.max(prev - 1, 0));
                  } else if (e.key === 'Enter' && modalDestBankDropdownOpen && filteredDestBanks.length > 0) {
                    e.preventDefault();
                    const selected = filteredDestBanks[modalDestBankHighlightIndex];
                    if (selected) {
                      setModalDestBank(selected.code);
                      setModalDestBankSearch(`${selected.code} - ${selected.description}`);
                      setModalDestBankDropdownOpen(false);
                      // Auto-focus Save button after selection
                      setTimeout(() => modalBankTransferSaveRef.current?.focus(), 50);
                    }
                  } else if (e.key === 'Escape') {
                    setModalDestBankDropdownOpen(false);
                  } else if (e.key === 'Tab' && modalDestBankDropdownOpen) {
                    setModalDestBankDropdownOpen(false);
                  }
                }}
                placeholder="Search by code, name or sort code..."
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                ref={modalDestBankInputRef}
              />
              {modalDestBankDropdownOpen && (
                <>
                  <div
                    className="fixed inset-0 z-40"
                    onClick={() => setModalDestBankDropdownOpen(false)}
                  />
                  <div className="absolute z-50 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-y-auto">
                    {filteredDestBanks.map((b, idx) => (
                        <button
                          key={b.code}
                          type="button"
                          onClick={() => {
                            setModalDestBank(b.code);
                            setModalDestBankSearch(`${b.code} - ${b.description}`);
                            setModalDestBankDropdownOpen(false);
                          }}
                          className={`w-full text-left px-3 py-2 text-sm ${
                            idx === modalDestBankHighlightIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                          } ${modalDestBank === b.code ? 'text-blue-800' : ''}`}
                        >
                          <div>
                            <span className="font-medium">{b.code}</span>
                            <span className="text-gray-600"> - {b.description}</span>
                          </div>
                          {b.sort_code && (
                            <div className="text-xs text-gray-500">
                              Sort: {b.sort_code} {b.account_number && `| Acc: ${b.account_number}`}
                            </div>
                          )}
                        </button>
                      ))}
                    {filteredDestBanks.length === 0 && (
                      <div className="px-3 py-2 text-sm text-gray-500">No matching bank accounts found</div>
                    )}
                  </div>
                </>
              )}
              {selectedDestBank && (
                <div className="mt-2 text-xs text-gray-500">
                  {selectedDestBank.sort_code && <span>Sort: {selectedDestBank.sort_code} </span>}
                  {selectedDestBank.account_number && <span>Acc: {selectedDestBank.account_number}</span>}
                </div>
              )}
            </div>
              );
            })()}

            {/* Summary */}
            <div className="pt-2 border-t border-gray-200">
              <div className="flex justify-between items-center text-sm">
                <span className="text-gray-600">From:</span>
                <span className="font-medium">{isOutgoing ? selectedBankCode : modalDestBank || '?'}</span>
              </div>
              <div className="flex justify-between items-center text-sm mt-1">
                <span className="text-gray-600">To:</span>
                <span className="font-medium">{isOutgoing ? modalDestBank || '?' : selectedBankCode}</span>
              </div>
              <div className="flex justify-between items-center mt-2">
                <span className="text-gray-600">Amount:</span>
                <span className="text-lg font-bold text-blue-600">£{Math.abs(amount).toFixed(2)}</span>
              </div>
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 bg-gray-50 border-t border-gray-200 flex justify-end gap-3">
            <button
              onClick={() => setBankTransferModal({ open: false, transaction: null, source: 'unmatched' })}
              className="px-4 py-2 text-sm text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              ref={modalBankTransferSaveRef}
              onClick={() => handleSaveBankTransfer()}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && canSave) {
                  e.preventDefault();
                  handleSaveBankTransfer();
                }
              }}
              disabled={!canSave}
              className={`px-4 py-2 text-sm text-white rounded-md ${
                canSave
                  ? 'bg-blue-600 hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2'
                  : 'bg-gray-300 cursor-not-allowed'
              }`}
            >
              Save & Include
            </button>
          </div>
        </div>
      </div>
    );
  };

  // Render the Nominal Detail Modal
  const renderNominalDetailModal = () => {
    if (!nominalDetailModal.open || !nominalDetailModal.transaction) return null;

    const txn = nominalDetailModal.transaction;
    const grossAmount = Math.abs(txn.amount);
    const isReceipt = nominalDetailModal.transactionType === 'nominal_receipt';

    // Use component-level state (initialized in openNominalDetailModal)
    const selectedVat = vatCodes.find(v => v.code === modalVatCode);
    const vatRate = selectedVat?.rate || 0;

    // Calculate VAT from net when VAT code changes
    const handleVatCodeChange = (code: string) => {
      setModalVatCode(code);
      // N/A means no VAT applicable - set to 0
      if (code === 'N/A') {
        setModalVatAmount('0.00');
        return;
      }
      const vat = vatCodes.find(v => v.code === code);
      if (vat && parseFloat(modalNetAmount) > 0) {
        const net = parseFloat(modalNetAmount);
        const vatAmt = net * (vat.rate / 100);
        setModalVatAmount(vatAmt.toFixed(2));
      } else {
        setModalVatAmount('0.00');
      }
    };

    // Calculate VAT when net amount changes
    const handleNetAmountChange = (value: string) => {
      setModalNetAmount(value);
      if (selectedVat && parseFloat(value) > 0) {
        const net = parseFloat(value);
        const vatAmt = net * (selectedVat.rate / 100);
        setModalVatAmount(vatAmt.toFixed(2));
      }
    };

    // Calculate net from gross (reverse VAT calculation)
    const calculateNetFromGross = () => {
      if (selectedVat) {
        const net = grossAmount / (1 + selectedVat.rate / 100);
        setModalNetAmount(net.toFixed(2));
        setModalVatAmount((grossAmount - net).toFixed(2));
      }
    };

    const calculatedGross = (parseFloat(modalNetAmount) || 0) + (parseFloat(modalVatAmount) || 0);
    const nominalDesc = nominalAccounts.find(n => n.code === modalNominalCode)?.description || '';

    const canSave = modalNominalCode && modalVatCode && parseFloat(modalNetAmount) > 0;

    return (
      <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
        <div className="bg-white rounded-lg shadow-xl w-full max-w-lg mx-4">
          {/* Header */}
          <div className={`px-6 py-4 border-b ${isReceipt ? 'bg-green-50 border-green-200' : 'bg-red-50 border-red-200'}`}>
            <div className="flex justify-between items-center">
              <h3 className={`text-lg font-semibold ${isReceipt ? 'text-green-800' : 'text-red-800'}`}>
                {isReceipt ? 'Nominal Receipt' : 'Nominal Payment'} Details
              </h3>
              <button
                onClick={() => setNominalDetailModal({ open: false, transaction: null, transactionType: null, source: 'unmatched' })}
                className="text-gray-400 hover:text-gray-600"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            <div className="mt-2 text-sm text-gray-600">
              <div className="flex justify-between">
                <span>{txn.name}</span>
                <span className={`font-medium ${isReceipt ? 'text-green-700' : 'text-red-700'}`}>
                  £{grossAmount.toFixed(2)}
                </span>
              </div>
              <div className="text-xs text-gray-500 mt-1">{txn.date}</div>
            </div>
          </div>

          {/* Form */}
          <div className="px-6 py-4 space-y-4">
            {/* Nominal Account - Searchable */}
            {(() => {
              const filteredNominals = nominalAccounts
                .filter(n => {
                  if (!modalNominalSearch) return true;
                  const search = modalNominalSearch.toLowerCase();
                  return n.code.toLowerCase().includes(search) ||
                         n.description.toLowerCase().includes(search);
                })
                .slice(0, 50);
              return (
            <div className="relative">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Nominal Account <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                value={modalNominalSearch}
                onChange={(e) => {
                  setModalNominalSearch(e.target.value);
                  setModalNominalDropdownOpen(true);
                  setModalNominalHighlightIndex(0);
                  // Clear selection if user edits the text
                  if (modalNominalCode) {
                    const selected = nominalAccounts.find(n => n.code === modalNominalCode);
                    if (selected && e.target.value !== `${selected.code} - ${selected.description}`) {
                      setModalNominalCode('');
                    }
                  }
                }}
                onFocus={() => {
                  setModalNominalDropdownOpen(true);
                  setModalNominalHighlightIndex(0);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    if (!modalNominalDropdownOpen) {
                      setModalNominalDropdownOpen(true);
                    } else {
                      setModalNominalHighlightIndex(prev => Math.min(prev + 1, filteredNominals.length - 1));
                    }
                  } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    setModalNominalHighlightIndex(prev => Math.max(prev - 1, 0));
                  } else if (e.key === 'Enter' && modalNominalDropdownOpen && filteredNominals.length > 0) {
                    e.preventDefault();
                    const selected = filteredNominals[modalNominalHighlightIndex];
                    if (selected) {
                      setModalNominalCode(selected.code);
                      setModalNominalSearch(`${selected.code} - ${selected.description}`);
                      setModalNominalDropdownOpen(false);
                      // Auto-focus next field (VAT code)
                      setTimeout(() => modalVatInputRef.current?.focus(), 50);
                    }
                  } else if (e.key === 'Escape') {
                    setModalNominalDropdownOpen(false);
                  } else if (e.key === 'Tab' && modalNominalDropdownOpen) {
                    // Close dropdown on Tab and let normal tab behavior happen
                    setModalNominalDropdownOpen(false);
                  }
                }}
                placeholder="Search by code or description..."
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                autoFocus
                tabIndex={1}
              />
              {modalNominalDropdownOpen && (
                <>
                  <div
                    className="fixed inset-0 z-40"
                    onClick={() => setModalNominalDropdownOpen(false)}
                  />
                  <div className="absolute z-50 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-y-auto">
                    {filteredNominals.map((n, idx) => (
                        <button
                          key={n.code}
                          type="button"
                          onClick={() => {
                            setModalNominalCode(n.code);
                            setModalNominalSearch(`${n.code} - ${n.description}`);
                            setModalNominalDropdownOpen(false);
                          }}
                          className={`w-full text-left px-3 py-2 text-sm ${
                            idx === modalNominalHighlightIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                          } ${modalNominalCode === n.code ? 'text-blue-800' : ''}`}
                        >
                          <span className="font-medium">{n.code}</span>
                          <span className="text-gray-600"> - {n.description}</span>
                        </button>
                      ))}
                    {filteredNominals.length === 0 && (
                      <div className="px-3 py-2 text-sm text-gray-500">No matching accounts found</div>
                    )}
                  </div>
                </>
              )}
            </div>
              );
            })()}

            {/* VAT Code - Searchable */}
            {(() => {
              // Include N/A as first option if it matches search
              const showNa = !modalVatSearch || 'n/a'.includes(modalVatSearch.toLowerCase());
              const filteredVatCodes = vatCodes.filter(v => {
                if (!modalVatSearch) return true;
                const search = modalVatSearch.toLowerCase();
                return v.code.toLowerCase().includes(search) ||
                       v.description.toLowerCase().includes(search);
              });
              // Build combined list for keyboard navigation (N/A first if shown, then VAT codes)
              const allOptions: Array<{ code: string; label: string; isNa?: boolean }> = [];
              if (showNa) {
                allOptions.push({ code: 'N/A', label: 'N/A - No VAT applicable', isNa: true });
              }
              filteredVatCodes.forEach(v => {
                allOptions.push({ code: v.code, label: `${v.code} - ${v.description} (${v.rate}%)` });
              });
              return (
            <div className="relative">
              <label className="block text-sm font-medium text-gray-700 mb-1">
                VAT Code <span className="text-red-500">*</span>
              </label>
              <input
                ref={modalVatInputRef}
                type="text"
                value={modalVatSearch}
                onChange={(e) => {
                  setModalVatSearch(e.target.value);
                  setModalVatDropdownOpen(true);
                  setModalVatHighlightIndex(0);
                  // Clear selection if user edits the text
                  if (modalVatCode) {
                    setModalVatCode('');
                  }
                }}
                onFocus={() => {
                  setModalVatDropdownOpen(true);
                  setModalVatHighlightIndex(0);
                }}
                onKeyDown={(e) => {
                  if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    if (!modalVatDropdownOpen) {
                      setModalVatDropdownOpen(true);
                    } else {
                      setModalVatHighlightIndex(prev => Math.min(prev + 1, allOptions.length - 1));
                    }
                  } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    setModalVatHighlightIndex(prev => Math.max(prev - 1, 0));
                  } else if (e.key === 'Enter' && modalVatDropdownOpen && allOptions.length > 0) {
                    e.preventDefault();
                    const selected = allOptions[modalVatHighlightIndex];
                    if (selected) {
                      handleVatCodeChange(selected.code);
                      setModalVatSearch(selected.label);
                      setModalVatDropdownOpen(false);
                      // Auto-focus next field (Net Amount)
                      setTimeout(() => modalNetAmountRef.current?.focus(), 50);
                    }
                  } else if (e.key === 'Escape') {
                    setModalVatDropdownOpen(false);
                  } else if (e.key === 'Tab' && modalVatDropdownOpen) {
                    setModalVatDropdownOpen(false);
                  }
                }}
                placeholder="Search by code or description..."
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                tabIndex={2}
              />
              {modalVatDropdownOpen && (
                <>
                  <div
                    className="fixed inset-0 z-40"
                    onClick={() => setModalVatDropdownOpen(false)}
                  />
                  <div className="absolute z-50 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-y-auto">
                    {allOptions.map((opt, idx) => (
                      <button
                        key={opt.code}
                        type="button"
                        onClick={() => {
                          handleVatCodeChange(opt.code);
                          setModalVatSearch(opt.label);
                          setModalVatDropdownOpen(false);
                        }}
                        className={`w-full text-left px-3 py-2 text-sm ${
                          idx === modalVatHighlightIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                        } ${modalVatCode === opt.code ? 'text-blue-800' : ''}`}
                      >
                        <span className="font-medium">{opt.code}</span>
                        <span className="text-gray-600"> - {opt.isNa ? 'No VAT applicable' : opt.label.split(' - ')[1]}</span>
                      </button>
                    ))}
                    {allOptions.length === 0 && (
                      <div className="px-3 py-2 text-sm text-gray-500">No matching VAT codes found</div>
                    )}
                  </div>
                </>
              )}
            </div>
              );
            })()}

            {/* Net Amount */}
            <div className="flex gap-4">
              <div className="flex-1">
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Net Amount <span className="text-red-500">*</span>
                </label>
                <div className="relative">
                  <span className="absolute left-3 top-2 text-gray-500">£</span>
                  <input
                    ref={modalNetAmountRef}
                    type="number"
                    step="0.01"
                    value={modalNetAmount}
                    onChange={(e) => handleNetAmountChange(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        // Focus Save button when pressing Enter on Net Amount
                        modalSaveButtonRef.current?.focus();
                      }
                    }}
                    className="w-full pl-7 pr-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    tabIndex={3}
                  />
                </div>
              </div>

              <div className="flex-1">
                <label className={`block text-sm font-medium mb-1 ${modalVatCode === 'N/A' ? 'text-gray-400' : 'text-gray-700'}`}>
                  VAT Amount
                </label>
                <div className="relative">
                  <span className={`absolute left-3 top-2 ${modalVatCode === 'N/A' ? 'text-gray-300' : 'text-gray-500'}`}>£</span>
                  <input
                    type="number"
                    step="0.01"
                    value={modalVatAmount}
                    onChange={(e) => setModalVatAmount(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault();
                        modalSaveButtonRef.current?.focus();
                      }
                    }}
                    disabled={modalVatCode === 'N/A'}
                    className={`w-full pl-7 pr-3 py-2 border rounded-md ${
                      modalVatCode === 'N/A'
                        ? 'bg-gray-100 border-gray-200 text-gray-400 cursor-not-allowed'
                        : 'border-gray-300 focus:ring-2 focus:ring-blue-500 focus:border-blue-500'
                    }`}
                    tabIndex={4}
                  />
                </div>
              </div>
            </div>

            {/* Quick calc button */}
            {selectedVat && selectedVat.rate > 0 && (
              <button
                type="button"
                onClick={calculateNetFromGross}
                className="text-sm text-blue-600 hover:text-blue-800 underline"
              >
                Calculate net from gross (£{grossAmount.toFixed(2)} @ {selectedVat.rate}% VAT)
              </button>
            )}

            {/* Gross total */}
            <div className="flex justify-between items-center pt-2 border-t border-gray-200">
              <span className="text-sm font-medium text-gray-600">Gross Total:</span>
              <span className={`text-lg font-bold ${
                Math.abs(calculatedGross - grossAmount) < 0.01 ? 'text-green-600' : 'text-orange-600'
              }`}>
                £{calculatedGross.toFixed(2)}
                {Math.abs(calculatedGross - grossAmount) >= 0.01 && (
                  <span className="text-xs font-normal ml-2 text-orange-500">
                    (Txn: £{grossAmount.toFixed(2)})
                  </span>
                )}
              </span>
            </div>
          </div>

          {/* Footer */}
          <div className="px-6 py-4 bg-gray-50 border-t border-gray-200 flex justify-end gap-3">
            <button
              onClick={() => setNominalDetailModal({ open: false, transaction: null, transactionType: null, source: 'unmatched' })}
              className="px-4 py-2 text-sm text-gray-700 bg-white border border-gray-300 rounded-md hover:bg-gray-50"
              tabIndex={6}
            >
              Cancel
            </button>
            <button
              ref={modalSaveButtonRef}
              onClick={() => handleSaveNominalDetail({
                nominalCode: modalNominalCode,
                nominalDescription: nominalDesc,
                vatCode: modalVatCode,
                vatRate: vatRate,
                netAmount: parseFloat(modalNetAmount) || 0,
                vatAmount: parseFloat(modalVatAmount) || 0,
                grossAmount: calculatedGross
              })}
              onKeyDown={(e) => {
                if (e.key === 'Enter' && canSave) {
                  e.preventDefault();
                  handleSaveNominalDetail({
                    nominalCode: modalNominalCode,
                    nominalDescription: nominalDesc,
                    vatCode: modalVatCode,
                    vatRate: vatRate,
                    netAmount: parseFloat(modalNetAmount) || 0,
                    vatAmount: parseFloat(modalVatAmount) || 0,
                    grossAmount: calculatedGross
                  });
                }
              }}
              disabled={!canSave}
              className={`px-4 py-2 text-sm text-white rounded-md ${
                canSave
                  ? 'bg-blue-600 hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2'
                  : 'bg-gray-300 cursor-not-allowed'
              }`}
              tabIndex={5}
            >
              Save & Include
            </button>
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-6">
      {/* Nominal Detail Modal */}
      {renderNominalDetailModal()}
      {/* Bank Transfer Modal */}
      {renderBankTransferModal()}

      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">{bankRecOnly ? 'Bank Statement Import' : 'Imports'}</h1>
        <p className="text-gray-600 mt-1">Import and reconcile bank statement transactions</p>
      </div>

      {/* Warning: Reconciliation in progress in Opera */}
      {reconciliationStatus?.reconciliation_in_progress && (
        <div className="bg-red-50 border border-red-300 rounded-lg p-4 flex items-start gap-3">
          <span className="text-red-500 text-xl">⚠</span>
          <div className="flex-1">
            <h3 className="font-semibold text-red-800">Reconciliation In Progress in Opera</h3>
            <p className="text-red-700 text-sm mt-1">
              {reconciliationStatus.reconciliation_in_progress_message ||
               `There are ${reconciliationStatus.partial_entries || 0} entries marked as reconciled but not yet posted in Opera. Please complete or clear the reconciliation in Opera before importing new statements.`}
            </p>
          </div>
        </div>
      )}

      {/* Import Type Selector - hidden in bankRecOnly mode */}
      {!bankRecOnly && (
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex flex-wrap gap-2">
            {importTypes.map(type => {
              const Icon = type.icon;
              const isActive = activeType === type.id;
              return (
                <button
                  key={type.id}
                  onClick={() => { setActiveType(type.id); resetForm(); }}
                  className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-colors ${
                    isActive
                      ? `bg-${type.color}-100 text-${type.color}-700 border-2 border-${type.color}-500`
                      : 'bg-gray-100 text-gray-600 hover:bg-gray-200 border-2 border-transparent'
                  }`}
                >
                  <Icon className="h-4 w-4" />
                  {type.label}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Bank Statement Import Form */}
      {activeType === 'bank-statement' && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4">
            Bank Statement Import
          </h2>

          {/* Workflow Steps Indicator */}
          <div className="mb-6 p-4 bg-gradient-to-r from-blue-50 to-purple-50 border border-blue-200 rounded-lg">
            <div className="flex items-center justify-between">
              {/* Step 1: Select & Scan */}
              <div className={`flex items-center gap-2 ${!bankPreview ? 'text-blue-700 font-semibold' : 'text-gray-400'}`}>
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${
                  !bankPreview ? 'bg-blue-600 text-white' : 'bg-green-500 text-white'
                }`}>
                  {bankPreview ? '✓' : '1'}
                </div>
                <div className="text-sm">
                  <div className="font-medium">Select Statement</div>
                  <div className="text-xs text-gray-500">Choose source & scan</div>
                </div>
              </div>

              <div className="flex-1 h-1 mx-2 bg-gray-200 rounded">
                <div className={`h-1 rounded transition-all ${bankPreview ? 'w-full bg-green-500' : 'w-0'}`} />
              </div>

              {/* Step 2: Analyse */}
              <div className={`flex items-center gap-2 ${bankPreview && !bankImportResult ? 'text-blue-700 font-semibold' : bankImportResult ? 'text-gray-400' : 'text-gray-400'}`}>
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${
                  bankImportResult ? 'bg-green-500 text-white' : bankPreview ? 'bg-blue-600 text-white' : 'bg-gray-300 text-gray-500'
                }`}>
                  {bankImportResult ? '✓' : '2'}
                </div>
                <div className="text-sm">
                  <div className="font-medium">Review & Assign</div>
                  <div className="text-xs text-gray-500">Check matches, assign accounts</div>
                </div>
              </div>

              <div className="flex-1 h-1 mx-2 bg-gray-200 rounded">
                <div className={`h-1 rounded transition-all ${bankImportResult ? 'w-full bg-green-500' : 'w-0'}`} />
              </div>

              {/* Step 3: Import */}
              <div className={`flex items-center gap-2 ${bankImportResult && !bankImportResult.success ? 'text-blue-700 font-semibold' : bankImportResult?.success ? 'text-gray-400' : 'text-gray-400'}`}>
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${
                  bankImportResult?.success ? 'bg-green-500 text-white' : bankImportResult ? 'bg-blue-600 text-white' : 'bg-gray-300 text-gray-500'
                }`}>
                  {bankImportResult?.success ? '✓' : '3'}
                </div>
                <div className="text-sm">
                  <div className="font-medium">Import</div>
                  <div className="text-xs text-gray-500">Post to Opera</div>
                </div>
              </div>

              <div className="flex-1 h-1 mx-2 bg-gray-200 rounded">
                <div className={`h-1 rounded transition-all ${bankImportResult?.success ? 'w-full bg-green-500' : 'w-0'}`} />
              </div>

              {/* Step 4: Reconcile */}
              <div className={`flex items-center gap-2 ${bankImportResult?.success && showReconcilePrompt ? 'text-blue-700 font-semibold' : 'text-gray-400'}`}>
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${
                  bankImportResult?.success && !showReconcilePrompt ? 'bg-green-500 text-white' : bankImportResult?.success ? 'bg-blue-600 text-white' : 'bg-gray-300 text-gray-500'
                }`}>
                  {bankImportResult?.success && !showReconcilePrompt ? '✓' : '4'}
                </div>
                <div className="text-sm">
                  <div className="font-medium">Reconcile</div>
                  <div className="text-xs text-gray-500">Match to statement</div>
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-6">
            {/* Data source indicator */}
            <div className="flex items-center gap-2 p-3 bg-gray-50 rounded-lg">
              <span className="text-sm font-medium text-gray-700">Data Source:</span>
              <span className="text-sm font-semibold text-blue-700">
                {dataSource === 'opera-sql' ? 'Opera SQL SE' : 'Opera 3 (FoxPro)'}
              </span>
              <span className="text-xs text-gray-500">(configured in Settings)</span>
            </div>

            {/* Statement Source Toggle */}
            <div className="flex items-center justify-between gap-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
              <div className="flex items-center gap-4">
                <span className="text-sm font-medium text-gray-700">Statement Source:</span>
                <div className="flex gap-2">
                  <button
                    onClick={() => setStatementSource('email')}
                    disabled={!!bankPreview}
                    className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
                      statementSource === 'email'
                        ? 'bg-blue-600 text-white'
                        : bankPreview
                          ? 'bg-gray-100 text-gray-400 border border-gray-200 cursor-not-allowed'
                          : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                    }`}
                  >
                    <FileText className="h-4 w-4 inline-block mr-1.5" />
                    Email Inbox
                  </button>
                  <button
                    onClick={() => setStatementSource('pdf')}
                    disabled={!!bankPreview}
                    className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
                      statementSource === 'pdf'
                        ? 'bg-blue-600 text-white'
                        : bankPreview
                          ? 'bg-gray-100 text-gray-400 border border-gray-200 cursor-not-allowed'
                          : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                    }`}
                  >
                    <FileText className="h-4 w-4 inline-block mr-1.5" />
                    PDF Upload
                  </button>
                  {/* CSV Upload button hidden - code retained for future use
                  <button
                    onClick={() => setStatementSource('file')}
                    disabled={!!bankPreview}
                    className={`px-4 py-1.5 rounded-md text-sm font-medium transition-colors ${
                      statementSource === 'file'
                        ? 'bg-blue-600 text-white'
                        : bankPreview
                          ? 'bg-gray-100 text-gray-400 border border-gray-200 cursor-not-allowed'
                          : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                    }`}
                  >
                    <Upload className="h-4 w-4 inline-block mr-1.5" />
                    CSV Upload
                  </button>
                  */}
                </div>
              </div>
              {/* History Button */}
              <button
                onClick={() => setShowImportHistory(true)}
                className="px-4 py-1.5 rounded-md text-sm font-medium bg-gray-100 text-gray-700 border border-gray-300 hover:bg-gray-200 flex items-center gap-2"
              >
                <History className="h-4 w-4" />
                View History
              </button>
            </div>

            {/* Email Scanning Section */}
            {statementSource === 'email' && (
              <div className="space-y-4">
                {/* Bank Selection for Email Scan */}
                <div className="grid grid-cols-3 gap-4">
                  {(() => {
                    const filteredBanks = bankAccounts.filter(bank => {
                      if (!bankSelectSearch) return true;
                      const search = bankSelectSearch.toLowerCase();
                      return bank.code.toLowerCase().includes(search) ||
                             bank.description.toLowerCase().includes(search) ||
                             (bank.sort_code && bank.sort_code.includes(search));
                    });
                    return (
                  <div className="relative">
                    <label className="block text-sm font-medium text-gray-700 mb-1">Bank Account</label>
                    <input
                      type="text"
                      value={bankSelectOpen === 'email' ? bankSelectSearch : (
                        bankAccounts.find(b => b.code === selectedBankCode)
                          ? `${selectedBankCode} - ${bankAccounts.find(b => b.code === selectedBankCode)?.description}`
                          : ''
                      )}
                      onChange={(e) => {
                        setBankSelectSearch(e.target.value);
                        setBankSelectHighlightIndex(0);
                        if (bankSelectOpen !== 'email') setBankSelectOpen('email');
                      }}
                      onFocus={() => {
                        setBankSelectOpen('email');
                        setBankSelectSearch('');
                        setBankSelectHighlightIndex(0);
                      }}
                      onKeyDown={(e) => {
                        if (bankSelectOpen !== 'email') return;
                        if (e.key === 'ArrowDown') {
                          e.preventDefault();
                          setBankSelectHighlightIndex(prev => Math.min(prev + 1, filteredBanks.length - 1));
                        } else if (e.key === 'ArrowUp') {
                          e.preventDefault();
                          setBankSelectHighlightIndex(prev => Math.max(prev - 1, 0));
                        } else if (e.key === 'Enter' && filteredBanks.length > 0) {
                          e.preventDefault();
                          const selectedBank = filteredBanks[bankSelectHighlightIndex];
                          if (selectedBank) {
                            setSelectedBankCode(selectedBank.code);
                            setBankSelectOpen(null);
                            setBankSelectSearch('');
                          }
                        } else if (e.key === 'Escape') {
                          setBankSelectOpen(null);
                          setBankSelectSearch('');
                        }
                      }}
                      placeholder="Search bank account..."
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    />
                    {bankSelectOpen === 'email' && (
                      <>
                        <div className="fixed inset-0 z-40" onClick={() => { setBankSelectOpen(null); setBankSelectSearch(''); }} />
                        <div className="absolute z-50 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-y-auto">
                          {filteredBanks.map((bank, idx) => (
                              <button
                                key={bank.code}
                                type="button"
                                onClick={() => {
                                  setSelectedBankCode(bank.code);
                                  setBankSelectOpen(null);
                                  setBankSelectSearch('');
                                }}
                                className={`w-full text-left px-3 py-2 text-sm ${
                                  idx === bankSelectHighlightIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                                } ${selectedBankCode === bank.code ? 'text-blue-800' : ''}`}
                              >
                                <span className="font-medium">{bank.code}</span>
                                <span className="text-gray-600"> - {bank.description}</span>
                                {bank.sort_code && (
                                  <span className="text-gray-400 text-xs block">Sort: {bank.sort_code}</span>
                                )}
                              </button>
                            ))}
                        </div>
                      </>
                    )}
                  </div>
                    );
                  })()}
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Days Back</label>
                    <select
                      value={emailScanDaysBack}
                      onChange={e => setEmailScanDaysBack(parseInt(e.target.value))}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    >
                      <option value={7}>7 days</option>
                      <option value={14}>14 days</option>
                      <option value={30}>30 days</option>
                      <option value={60}>60 days</option>
                      <option value={90}>90 days</option>
                    </select>
                  </div>
                  <div className="flex items-end">
                    <button
                      onClick={handleScanEmails}
                      disabled={emailScanLoading || !!bankPreview}
                      className="w-full px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                      title={bankPreview ? 'Clear current statement first' : 'Step 1: Scan inbox for bank statements'}
                    >
                      {emailScanLoading ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <Search className="h-4 w-4" />
                      )}
                      <span className="font-medium">Step 1:</span> Scan Inbox
                    </button>
                  </div>
                </div>

                {/* Email Statements List - hide when statement is being previewed/imported */}
                {emailStatements.length > 0 && !bankPreview && (() => {
                  // Find the first unprocessed statement (the one to import next)
                  const firstUnprocessedIndex = emailStatements.findIndex(e => !e.already_processed);

                  return (
                  <div className="border border-gray-200 rounded-lg overflow-hidden">
                    <div className="bg-gray-50 px-4 py-2 border-b border-gray-200 flex justify-between items-center">
                      <span className="text-sm font-medium text-gray-700">
                        Found {emailStatements.length} statement(s) — import in order
                      </span>
                      {emailStatements.length > 1 && (
                        <span className="text-xs text-gray-500">
                          Statements ordered by date/number
                        </span>
                      )}
                    </div>
                    <div className="divide-y divide-gray-100 max-h-[600px] overflow-y-auto">
                      {emailStatements.map((email, index) => {
                        const isNextToImport = index === firstUnprocessedIndex;
                        const canImport = isNextToImport || email.already_processed;
                        const importSequence = (email as any).import_sequence || index + 1;
                        const statementDate = (email as any).statement_date;

                        return (
                        <div
                          key={email.email_id}
                          className={`p-4 transition-all ${
                            email.already_processed
                              ? 'bg-green-50 opacity-75'
                              : isNextToImport
                                ? 'bg-blue-50 border-l-4 border-l-blue-500'
                                : 'bg-gray-50 opacity-60'
                          }`}
                        >
                          <div className="flex justify-between items-start">
                            <div className="flex items-start gap-3 flex-1">
                              {/* Sequence number badge */}
                              <div className={`flex-shrink-0 w-8 h-8 rounded-full flex items-center justify-center text-sm font-bold ${
                                email.already_processed
                                  ? 'bg-green-500 text-white'
                                  : isNextToImport
                                    ? 'bg-blue-600 text-white'
                                    : 'bg-gray-300 text-gray-600'
                              }`}>
                                {email.already_processed ? '✓' : importSequence}
                              </div>

                              <div className="flex-1">
                                <div className="flex items-center gap-2 flex-wrap">
                                  {/* Show bank name or detected bank or filename */}
                                  <span className="font-medium text-gray-900">
                                    {(email as any).bank_name || email.detected_bank?.toUpperCase() || email.attachments?.[0]?.filename || email.subject || '(Unknown)'}
                                  </span>
                                  {/* Show statement period dates if available */}
                                  {((email as any).period_start || (email as any).period_end) ? (
                                    <span className="px-2 py-0.5 bg-purple-100 text-purple-700 text-xs rounded-full">
                                      {(() => {
                                        // Parse dates without timezone issues
                                        const formatDate = (dateStr: string) => {
                                          if (!dateStr) return '';
                                          // Extract just the date part (YYYY-MM-DD) in case there's a time component
                                          const datePart = dateStr.split(' ')[0].split('T')[0];
                                          const d = new Date(datePart + 'T12:00:00');
                                          return d.toLocaleDateString('en-GB');
                                        };
                                        const start = (email as any).period_start;
                                        const end = (email as any).period_end;
                                        if (start && end) {
                                          return `${formatDate(start)} - ${formatDate(end)}`;
                                        }
                                        return formatDate(end || start);
                                      })()}
                                    </span>
                                  ) : statementDate && (
                                    <span className="px-2 py-0.5 bg-purple-100 text-purple-700 text-xs rounded-full">
                                      {statementDate}
                                    </span>
                                  )}
                                  {isNextToImport && (
                                    <span className="px-2 py-0.5 bg-blue-600 text-white text-xs rounded-full">
                                      Next to import
                                    </span>
                                  )}
                                </div>
                                {/* Show opening/closing balance if available */}
                                <div className="text-sm text-gray-500 mt-1 flex items-center gap-3">
                                  {(email as any).opening_balance !== undefined && (email as any).opening_balance !== null ? (
                                    <>
                                      <span>Opening: £{(email as any).opening_balance?.toLocaleString('en-GB', { minimumFractionDigits: 2 })}</span>
                                      {(email as any).closing_balance !== undefined && (email as any).closing_balance !== null && (
                                        <span>→ Closing: £{(email as any).closing_balance?.toLocaleString('en-GB', { minimumFractionDigits: 2 })}</span>
                                      )}
                                    </>
                                  ) : (
                                    <span>{email.from_name || email.from_address}</span>
                                  )}
                                  <span className="text-gray-400">•</span>
                                  <span className="text-gray-400">{new Date(email.received_at).toLocaleDateString()}</span>
                                </div>
                                <div className="mt-2 space-y-1">
                                  {email.attachments.map(att => (
                                    <div
                                      key={att.attachment_id}
                                      className={`flex items-center justify-between px-3 py-1.5 rounded text-sm ${
                                        isNextToImport ? 'bg-white' : 'bg-gray-100'
                                      }`}
                                    >
                                      <div className="flex items-center gap-2">
                                        <FileText className="h-4 w-4 text-gray-400" />
                                        <span className="text-gray-700">{att.filename}</span>
                                        <span className="text-gray-400 text-xs">
                                          ({(att.size_bytes / 1024).toFixed(1)} KB)
                                        </span>
                                        {att.already_processed && (
                                          <span className="text-xs text-green-600 font-medium">(imported)</span>
                                        )}
                                        {(att as any).statement_date && !statementDate && (
                                          <span className="text-xs text-purple-600">({(att as any).statement_date})</span>
                                        )}
                                      </div>
                                      {!att.already_processed && (
                                        <div className="flex gap-1">
                                          <button
                                            onClick={() => handleEmailAttachmentRawPreview(email.email_id, att.attachment_id)}
                                            className="px-2 py-1 text-gray-600 text-xs rounded bg-gray-100 hover:bg-gray-200 border border-gray-300"
                                            title="View raw file contents"
                                          >
                                            View
                                          </button>
                                          <button
                                            onClick={() => handleEmailPreview(email.email_id, att.attachment_id, att.filename)}
                                            disabled={isPreviewing || !canImport || !!bankPreview}
                                            className={`px-3 py-1 text-white text-xs rounded ${
                                              isNextToImport && !bankPreview
                                                ? 'bg-blue-600 hover:bg-blue-700'
                                                : 'bg-gray-400 cursor-not-allowed'
                                            } disabled:bg-gray-400`}
                                            title={bankPreview ? 'Clear current statement first' : (!canImport ? 'Import previous statements first' : '')}
                                          >
                                            {isPreviewing && selectedEmailStatement?.attachmentId === att.attachment_id ? (
                                              <Loader2 className="h-3 w-3 animate-spin" />
                                            ) : (
                                              'Analyse'
                                            )}
                                          </button>
                                        </div>
                                      )}
                                    </div>
                                  ))}
                                </div>
                              </div>
                            </div>
                          </div>
                        </div>
                      )})}
                    </div>
                  </div>
                );
                })()}

                {emailStatements.length === 0 && !emailScanLoading && (
                  <div className="text-center py-8 text-gray-500">
                    <FileText className="h-12 w-12 mx-auto text-gray-300 mb-3" />
                    <p>Click "Scan for Statements" to search your inbox for bank statement attachments</p>
                  </div>
                )}
              </div>
            )}

            {/* PDF Upload Section - mirrors Email Inbox functionality */}
            {statementSource === 'pdf' && (
              <div className="space-y-4">
                {/* Bank Selection and Folder Path for PDF Scan */}
                <div className="grid grid-cols-3 gap-4">
                  {(() => {
                    const filteredBanks = bankAccounts.filter(bank => {
                      if (!bankSelectSearch) return true;
                      const search = bankSelectSearch.toLowerCase();
                      return bank.code.toLowerCase().includes(search) ||
                             bank.description.toLowerCase().includes(search) ||
                             (bank.sort_code && bank.sort_code.includes(search));
                    });
                    return (
                  <div className="relative">
                    <label className="block text-sm font-medium text-gray-700 mb-1">Bank Account</label>
                    <input
                      type="text"
                      value={bankSelectOpen === 'pdf' ? bankSelectSearch : (
                        bankAccounts.find(b => b.code === selectedBankCode)
                          ? `${selectedBankCode} - ${bankAccounts.find(b => b.code === selectedBankCode)?.description}`
                          : ''
                      )}
                      onChange={(e) => {
                        setBankSelectSearch(e.target.value);
                        setBankSelectHighlightIndex(0);
                        if (bankSelectOpen !== 'pdf') setBankSelectOpen('pdf');
                      }}
                      onFocus={() => {
                        setBankSelectOpen('pdf');
                        setBankSelectSearch('');
                        setBankSelectHighlightIndex(0);
                      }}
                      onKeyDown={(e) => {
                        if (bankSelectOpen !== 'pdf') return;
                        if (e.key === 'ArrowDown') {
                          e.preventDefault();
                          setBankSelectHighlightIndex(prev => Math.min(prev + 1, filteredBanks.length - 1));
                        } else if (e.key === 'ArrowUp') {
                          e.preventDefault();
                          setBankSelectHighlightIndex(prev => Math.max(prev - 1, 0));
                        } else if (e.key === 'Enter' && filteredBanks.length > 0) {
                          e.preventDefault();
                          const selectedBank = filteredBanks[bankSelectHighlightIndex];
                          if (selectedBank) {
                            setSelectedBankCode(selectedBank.code);
                            setBankSelectOpen(null);
                            setBankSelectSearch('');
                          }
                        } else if (e.key === 'Escape') {
                          setBankSelectOpen(null);
                          setBankSelectSearch('');
                        }
                      }}
                      placeholder="Search bank account..."
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    />
                    {bankSelectOpen === 'pdf' && (
                      <>
                        <div className="fixed inset-0 z-40" onClick={() => { setBankSelectOpen(null); setBankSelectSearch(''); }} />
                        <div className="absolute z-50 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-y-auto">
                          {filteredBanks.map((bank, idx) => (
                              <button
                                key={bank.code}
                                type="button"
                                onClick={() => {
                                  setSelectedBankCode(bank.code);
                                  setBankSelectOpen(null);
                                  setBankSelectSearch('');
                                }}
                                className={`w-full text-left px-3 py-2 text-sm ${
                                  idx === bankSelectHighlightIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                                } ${selectedBankCode === bank.code ? 'text-blue-800' : ''}`}
                              >
                                <span className="font-medium">{bank.code}</span>
                                <span className="text-gray-600"> - {bank.description}</span>
                                {bank.sort_code && (
                                  <span className="text-gray-400 text-xs block">Sort: {bank.sort_code}</span>
                                )}
                              </button>
                            ))}
                        </div>
                      </>
                    )}
                  </div>
                    );
                  })()}
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">PDF Folder Path</label>
                    <input
                      type="text"
                      value={pdfDirectory}
                      onChange={e => setPdfDirectory(e.target.value)}
                      className="w-full px-3 py-2 bg-white border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      placeholder="e.g. C:\Bank Statements"
                    />
                  </div>
                  <div className="flex items-end">
                    <button
                      onClick={handleScanPdfFiles}
                      disabled={pdfFilesLoading || !pdfDirectory || !!bankPreview}
                      className="w-full px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center justify-center gap-2"
                      title={bankPreview ? 'Clear current statement first' : 'Step 1: Scan folder for PDF statements'}
                    >
                      {pdfFilesLoading ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <Search className="h-4 w-4" />
                      )}
                      <span className="font-medium">Step 1:</span> Scan Folder
                    </button>
                  </div>
                </div>

                {/* PDF Files List - hide when statement is being previewed/imported */}
                {pdfFilesList && pdfFilesList.length > 0 && !bankPreview && (
                  <div className="border border-gray-200 rounded-lg overflow-hidden">
                    <div className="bg-gray-50 px-4 py-2 border-b border-gray-200">
                      <h4 className="font-medium text-gray-700 text-sm">
                        Found {pdfFilesList.length} PDF file{pdfFilesList.length !== 1 ? 's' : ''} in folder
                      </h4>
                    </div>
                    <div className="divide-y divide-gray-100 max-h-80 overflow-y-auto">
                      {pdfFilesList.map((file, idx) => {
                        const isNextToImport = !file.already_processed &&
                          (idx === 0 || pdfFilesList.slice(0, idx).every(f => f.already_processed));
                        const canImport = !file.already_processed;

                        return (
                          <div
                            key={file.filename}
                            className={`p-3 hover:bg-gray-50 ${file.already_processed ? 'bg-green-50/50' : ''}`}
                          >
                            <div className="flex items-center justify-between">
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2">
                                  <FileText className={`h-4 w-4 flex-shrink-0 ${file.already_processed ? 'text-green-600' : 'text-red-500'}`} />
                                  <span className={`font-medium truncate ${file.already_processed ? 'text-green-700' : 'text-gray-900'}`}>
                                    {file.filename}
                                  </span>
                                  {file.already_processed && (
                                    <span className="text-xs bg-green-100 text-green-700 px-2 py-0.5 rounded-full">
                                      Imported
                                    </span>
                                  )}
                                  {isNextToImport && !file.already_processed && (
                                    <span className="text-xs bg-blue-100 text-blue-700 px-2 py-0.5 rounded-full">
                                      Next
                                    </span>
                                  )}
                                </div>
                                <div className="text-xs text-gray-500 mt-0.5">
                                  {file.modified} • {file.size_display}
                                  {file.statement_date && ` • Statement: ${new Date(file.statement_date).toLocaleDateString('en-GB')}`}
                                </div>
                              </div>
                              <div className="flex items-center gap-2 ml-4">
                                <button
                                  onClick={() => handlePdfFileView(file.filename)}
                                  className="px-2 py-1 text-gray-600 text-xs rounded bg-gray-100 hover:bg-gray-200 border border-gray-300"
                                  title="View PDF file"
                                >
                                  View
                                </button>
                                {file.already_processed ? (
                                  <span className="text-xs text-green-600 flex items-center gap-1">
                                    <CheckCircle className="h-3 w-3" />
                                    Processed
                                  </span>
                                ) : (
                                  <button
                                    onClick={() => handlePdfPreview(file.filename)}
                                    disabled={isPreviewing || !canImport || !!bankPreview}
                                    className={`px-3 py-1 text-white text-xs rounded ${
                                      isNextToImport && !bankPreview
                                        ? 'bg-blue-600 hover:bg-blue-700'
                                        : 'bg-gray-400 cursor-not-allowed'
                                    } disabled:bg-gray-400`}
                                    title={bankPreview ? 'Clear current statement first' : (!canImport ? 'Import previous statements first' : '')}
                                  >
                                    {isPreviewing && selectedPdfFile?.filename === file.filename ? (
                                      <Loader2 className="h-3 w-3 animate-spin" />
                                    ) : (
                                      'Analyse'
                                    )}
                                  </button>
                                )}
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}

                {pdfFilesList && pdfFilesList.length === 0 && (
                  <div className="text-center py-8 text-gray-500 border border-gray-200 rounded-lg">
                    <FileText className="h-12 w-12 mx-auto text-gray-300 mb-3" />
                    <p>No PDF files found in the specified folder</p>
                  </div>
                )}

                {!pdfFilesList && !pdfFilesLoading && (
                  <div className="text-center py-8 text-gray-500">
                    <FileText className="h-12 w-12 mx-auto text-gray-300 mb-3" />
                    <p>Enter a folder path and click "Scan for PDFs" to find bank statement PDFs</p>
                  </div>
                )}
              </div>
            )}

            {/* CSV File Selection - FIRST (file contains bank details) */}
            {statementSource === 'file' && (
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">CSV Folder Path</label>
                  <input
                    type="text"
                    value={csvDirectory}
                    onChange={e => setCsvDirectory(e.target.value)}
                    className="w-full px-3 py-2 bg-white border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="e.g. C:\Downloads"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">CSV File</label>
                  {csvFilesList && csvFilesList.length > 0 ? (
                    <select
                      value={csvFileName}
                      onChange={e => setCsvFileName(e.target.value)}
                      className="w-full px-3 py-2 bg-white border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    >
                      <option value="">Select a CSV file...</option>
                      {csvFilesList.map((f: any) => (
                        <option key={f.filename} value={f.filename}>
                          {f.filename} — {f.modified} ({f.size_display})
                        </option>
                      ))}
                    </select>
                  ) : (
                    <input
                      type="text"
                      value={csvFileName}
                      onChange={e => setCsvFileName(e.target.value)}
                      className="w-full px-3 py-2 bg-white border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      placeholder="Enter filename or enter folder path above"
                    />
                  )}
                </div>
              </div>

              {/* Bank Account Display - Auto-detected from file */}
              {dataSource === 'opera-sql' ? (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Bank Account (from file)</label>
                  {detectedBank?.loading ? (
                    <div className="flex items-center gap-2 px-3 py-2 bg-blue-50 border border-blue-200 rounded-md text-blue-700">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      <span>Detecting bank from file...</span>
                    </div>
                  ) : detectedBank?.detected ? (
                    <div className="px-3 py-2 bg-green-50 border border-green-300 rounded-md">
                      <div className="flex items-center gap-2">
                        <CheckCircle className="h-5 w-5 text-green-600" />
                        <div>
                          <div className="font-semibold text-green-800">
                            {detectedBank.bank_code} - {detectedBank.bank_description}
                          </div>
                          {(detectedBank.sort_code || detectedBank.account_number) && (
                            <div className="text-xs text-green-600">
                              {detectedBank.sort_code} | {detectedBank.account_number}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                  ) : csvFilePath ? (
                    <div className="space-y-2">
                      <div className="px-3 py-2 bg-amber-50 border border-amber-300 rounded-md">
                        <div className="flex items-center gap-2 text-amber-700">
                          <AlertCircle className="h-4 w-4" />
                          <span className="text-sm">{detectedBank?.message || 'Could not detect bank from file'}</span>
                        </div>
                      </div>
                      {(() => {
                        const filteredBanks = bankAccounts.filter(bank => {
                          if (!bankSelectSearch) return true;
                          const search = bankSelectSearch.toLowerCase();
                          return bank.code.toLowerCase().includes(search) ||
                                 bank.description.toLowerCase().includes(search) ||
                                 (bank.sort_code && bank.sort_code.includes(search));
                        });
                        return (
                      <div className="relative">
                        <input
                          type="text"
                          value={bankSelectOpen === 'csv' ? bankSelectSearch : (
                            selectedBankCode && bankAccounts.find(b => b.code === selectedBankCode)
                              ? `${selectedBankCode} - ${bankAccounts.find(b => b.code === selectedBankCode)?.description}`
                              : ''
                          )}
                          onChange={(e) => {
                            setBankSelectSearch(e.target.value);
                            setBankSelectHighlightIndex(0);
                            if (bankSelectOpen !== 'csv') setBankSelectOpen('csv');
                          }}
                          onFocus={() => {
                            setBankSelectOpen('csv');
                            setBankSelectSearch('');
                            setBankSelectHighlightIndex(0);
                          }}
                          onKeyDown={(e) => {
                            if (bankSelectOpen !== 'csv') return;
                            if (e.key === 'ArrowDown') {
                              e.preventDefault();
                              setBankSelectHighlightIndex(prev => Math.min(prev + 1, filteredBanks.length - 1));
                            } else if (e.key === 'ArrowUp') {
                              e.preventDefault();
                              setBankSelectHighlightIndex(prev => Math.max(prev - 1, 0));
                            } else if (e.key === 'Enter' && filteredBanks.length > 0) {
                              e.preventDefault();
                              const selectedBank = filteredBanks[bankSelectHighlightIndex];
                              if (selectedBank) {
                                setSelectedBankCode(selectedBank.code);
                                setBankSelectOpen(null);
                                setBankSelectSearch('');
                              }
                            } else if (e.key === 'Escape') {
                              setBankSelectOpen(null);
                              setBankSelectSearch('');
                            }
                          }}
                          placeholder="Search bank account..."
                          className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                        />
                        {bankSelectOpen === 'csv' && (
                          <>
                            <div className="fixed inset-0 z-40" onClick={() => { setBankSelectOpen(null); setBankSelectSearch(''); }} />
                            <div className="absolute z-50 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-y-auto">
                              {filteredBanks.map((bank, idx) => (
                                  <button
                                    key={bank.code}
                                    type="button"
                                    onClick={() => {
                                      setSelectedBankCode(bank.code);
                                      setBankSelectOpen(null);
                                      setBankSelectSearch('');
                                    }}
                                    className={`w-full text-left px-3 py-2 text-sm ${
                                      idx === bankSelectHighlightIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                                    } ${selectedBankCode === bank.code ? 'text-blue-800' : ''}`}
                                  >
                                    <span className="font-medium">{bank.code}</span>
                                    <span className="text-gray-600"> - {bank.description}</span>
                                    {bank.sort_code && (
                                      <span className="text-gray-400 text-xs block">Sort: {bank.sort_code}</span>
                                    )}
                                  </button>
                                ))}
                            </div>
                          </>
                        )}
                      </div>
                        );
                      })()}
                    </div>
                  ) : (
                    <div className="px-3 py-2 bg-gray-50 border border-gray-200 rounded-md text-gray-500">
                      Select a CSV file to detect bank account
                    </div>
                  )}
                </div>
              ) : (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Opera 3 Data Path</label>
                  <input
                    type="text"
                    value={opera3DataPath}
                    onChange={e => setOpera3DataPath(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="/path/to/opera3/company/data"
                  />
                </div>
              )}
            </div>
            )}

            {/* Preview / Import Buttons */}
            {(() => {
              // For email/pdf source, use different handlers
              const handlePreviewClick = isEmailSource && selectedEmailStatement
                ? () => handleEmailPreview(selectedEmailStatement.emailId, selectedEmailStatement.attachmentId, selectedEmailStatement.filename)
                : isPdfSource && selectedPdfFile
                  ? () => handlePdfPreview(selectedPdfFile.filename)
                  : handleBankPreview;

              // Preview button disabled state varies by source - also disable if statement already loaded
              const previewDisabled = !!bankPreview || (isEmailSource
                ? (isPreviewing || noBankSelected || !selectedEmailStatement)
                : isPdfSource
                  ? (isPreviewing || noBankSelected || !selectedPdfFile)
                  : (isPreviewing || noBankSelected || !csvFilePath));

              return (
                <div className="space-y-3">
                  {/* Show source info when preview is loaded */}
                  {bankPreview && isEmailSource && selectedEmailStatement && (
                    <div className="flex items-center gap-2 px-3 py-2 bg-blue-50 border border-blue-200 rounded-md text-sm">
                      <FileText className="h-4 w-4 text-blue-600" />
                      <span className="text-blue-700">
                        Previewing: <strong>{selectedEmailStatement.filename}</strong> from email
                      </span>
                    </div>
                  )}

                  {bankPreview && isPdfSource && selectedPdfFile && (
                    <div className="flex items-center gap-2 px-3 py-2 bg-purple-50 border border-purple-200 rounded-md text-sm">
                      <FileText className="h-4 w-4 text-purple-600" />
                      <span className="text-purple-700">
                        Previewing: <strong>{selectedPdfFile.filename}</strong> from PDF upload
                      </span>
                    </div>
                  )}

                  {/* Statement Summary Table */}
                  {bankPreview?.statement_bank_info && (
                    <div className="bg-gray-50 border border-gray-200 rounded-lg overflow-hidden">
                      <div className="bg-gray-100 px-4 py-2 border-b border-gray-200">
                        <h4 className="font-medium text-gray-700 text-sm">Statement Summary</h4>
                      </div>
                      <div className="p-4">
                        <table className="w-full text-sm">
                          <tbody className="divide-y divide-gray-100">
                            {bankPreview.statement_bank_info.bank_name && (
                              <tr>
                                <td className="py-1.5 text-gray-500 w-40">Bank</td>
                                <td className="py-1.5 text-gray-900 font-medium">{bankPreview.statement_bank_info.bank_name}</td>
                              </tr>
                            )}
                            {(bankPreview.statement_bank_info.sort_code || bankPreview.statement_bank_info.account_number) && (
                              <tr>
                                <td className="py-1.5 text-gray-500">Account</td>
                                <td className="py-1.5 text-gray-900 font-mono">
                                  {bankPreview.statement_bank_info.sort_code && <span>{bankPreview.statement_bank_info.sort_code}</span>}
                                  {bankPreview.statement_bank_info.sort_code && bankPreview.statement_bank_info.account_number && ' / '}
                                  {bankPreview.statement_bank_info.account_number && <span>{bankPreview.statement_bank_info.account_number}</span>}
                                </td>
                              </tr>
                            )}
                            {bankPreview.statement_bank_info.statement_date && (
                              <tr>
                                <td className="py-1.5 text-gray-500">Statement Date</td>
                                <td className="py-1.5 text-gray-900">{(() => {
                                  const dateStr = bankPreview.statement_bank_info.statement_date || '';
                                  const datePart = dateStr.split(' ')[0].split('T')[0];
                                  return new Date(datePart + 'T12:00:00').toLocaleDateString('en-GB');
                                })()}</td>
                              </tr>
                            )}
                            {bankPreview.statement_bank_info.opening_balance !== undefined && (
                              <tr>
                                <td className="py-1.5 text-gray-500">Opening Balance</td>
                                <td className="py-1.5 text-gray-900 font-medium">
                                  £{bankPreview.statement_bank_info.opening_balance?.toLocaleString('en-GB', { minimumFractionDigits: 2 })}
                                </td>
                              </tr>
                            )}
                            {bankPreview.statement_bank_info.closing_balance !== undefined && (
                              <tr>
                                <td className="py-1.5 text-gray-500">Closing Balance</td>
                                <td className="py-1.5 text-gray-900 font-medium text-green-700">
                                  £{bankPreview.statement_bank_info.closing_balance?.toLocaleString('en-GB', { minimumFractionDigits: 2 })}
                                </td>
                              </tr>
                            )}
                            {bankPreview.statement_bank_info.matched_opera_bank && (
                              <tr>
                                <td className="py-1.5 text-gray-500">Opera Bank</td>
                                <td className="py-1.5">
                                  <span className="px-2 py-0.5 bg-green-100 text-green-700 rounded text-xs font-medium">
                                    {bankPreview.statement_bank_info.matched_opera_bank}
                                  </span>
                                  {bankPreview.statement_bank_info.matched_opera_name && (
                                    <span className="ml-2 text-gray-600">{bankPreview.statement_bank_info.matched_opera_name}</span>
                                  )}
                                </td>
                              </tr>
                            )}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}

                  <div className="flex gap-4">
                    {/* View File button for CSV source only (before analysis) */}
                    {!isEmailSource && !isPdfSource && (
                      <button
                        onClick={handleRawFilePreview}
                        disabled={!csvFilePath}
                        className="px-4 py-2 bg-gray-100 text-gray-700 rounded-md hover:bg-gray-200 disabled:bg-gray-50 disabled:text-gray-400 disabled:cursor-not-allowed flex items-center gap-2 border border-gray-300"
                        title="View raw file contents before processing"
                      >
                        <FileText className="h-4 w-4" />
                        View File
                      </button>
                    )}
                    {/* View Statement button for PDF/Email source (after selecting a file) */}
                    {(isPdfSource && selectedPdfFile) && (
                      <button
                        onClick={handleRawFilePreview}
                        className="px-4 py-2 bg-gray-100 text-gray-700 rounded-md hover:bg-gray-200 flex items-center gap-2 border border-gray-300"
                        title="View the PDF statement"
                      >
                        <FileText className="h-4 w-4" />
                        View Statement
                      </button>
                    )}
                    {(isEmailSource && selectedEmailStatement) && (
                      <button
                        onClick={handleRawFilePreview}
                        className="px-4 py-2 bg-gray-100 text-gray-700 rounded-md hover:bg-gray-200 flex items-center gap-2 border border-gray-300"
                        title="View the email attachment"
                      >
                        <FileText className="h-4 w-4" />
                        View Statement
                      </button>
                    )}
                    {/* Step 2: Analyse Transactions button - for all non-email sources */}
                    {!isEmailSource && (
                      <button
                        onClick={handlePreviewClick}
                        disabled={previewDisabled}
                        className={`px-6 py-2 rounded-md flex items-center gap-2 ${
                          previewDisabled
                            ? 'bg-gray-400 text-white cursor-not-allowed'
                            : 'bg-blue-600 text-white hover:bg-blue-700'
                        }`}
                        title={noBankSelected ? 'Select a file to detect bank account' : (!csvFilePath && !selectedPdfFile ? 'Select a file first' : 'Step 2: Analyse the statement')}
                      >
                        {isPreviewing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
                        <span className="font-medium">Step 2:</span> Analyse
                      </button>
                    )}
                    {/* Step 3 indicator - Update transactions (done in tables below) */}
                    {bankPreview && (
                      <div className="flex items-center gap-2 px-4 py-2 bg-amber-50 border-2 border-amber-300 rounded-md text-amber-800">
                        <Edit3 className="h-4 w-4" />
                        <span className="font-medium text-sm">Step 3:</span>
                        <span className="text-sm">Update transactions below</span>
                        <span className="text-xs text-amber-600 ml-2">→ then Import at bottom</span>
                      </div>
                    )}
                  </div>

                  {/* Raw File Preview Modal */}
                  {showRawPreview && rawFilePreview && (
                    <div className="bg-gray-50 border border-gray-200 rounded-lg overflow-hidden">
                      <div className="bg-gray-100 px-4 py-2 border-b border-gray-200 flex items-center justify-between">
                        <h4 className="font-medium text-gray-700 text-sm flex items-center gap-2">
                          <FileText className="h-4 w-4" />
                          Raw File Contents (first 50 lines)
                        </h4>
                        <button
                          onClick={() => setShowRawPreview(false)}
                          className="text-gray-500 hover:text-gray-700"
                        >
                          <X className="h-4 w-4" />
                        </button>
                      </div>
                      <div className="p-2 max-h-80 overflow-auto">
                        <pre className="text-xs font-mono text-gray-600 whitespace-pre-wrap">
                          {rawFilePreview.map((line, i) => (
                            <div key={i} className="hover:bg-gray-100 py-0.5 px-2">
                              <span className="text-gray-400 mr-3 select-none">{String(i + 1).padStart(3, ' ')}</span>
                              {line}
                            </div>
                          ))}
                        </pre>
                      </div>
                    </div>
                  )}

                  {/* PDF Viewer Modal */}
                  {pdfViewerData && (
                    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
                      <div className="bg-white rounded-lg shadow-xl w-[90vw] h-[90vh] flex flex-col">
                        <div className="bg-gray-100 px-4 py-3 border-b border-gray-200 flex items-center justify-between rounded-t-lg">
                          <h4 className="font-medium text-gray-700 flex items-center gap-2">
                            <FileText className="h-4 w-4" />
                            {pdfViewerData.filename}
                          </h4>
                          <button
                            onClick={() => setPdfViewerData(null)}
                            className="text-gray-500 hover:text-gray-700 p-1"
                          >
                            <X className="h-5 w-5" />
                          </button>
                        </div>
                        <div className="flex-1 overflow-hidden">
                          <iframe
                            src={pdfViewerData.viewUrl || `data:application/pdf;base64,${pdfViewerData.data}`}
                            className="w-full h-full"
                            title={pdfViewerData.filename}
                          />
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Import Readiness Summary */}
                  {importReadiness && bankPreview && (
                    <div className={`p-3 rounded-lg text-sm ${
                      hasUnhandledRepeatEntries ? 'bg-purple-50 border border-purple-200' :
                      hasPeriodViolations ? 'bg-orange-50 border border-orange-200' :
                      hasIncomplete ? 'bg-red-50 border border-red-200' :
                      importReadiness.totalReady > 0 ? 'bg-green-50 border border-green-200' :
                      'bg-gray-50 border border-gray-200'
                    }`}>
                      <div className="flex flex-wrap items-center gap-3">
                        <span className="font-medium">
                          {hasUnhandledRepeatEntries ? (
                            <span className="text-purple-700 flex items-center gap-1">
                              <RefreshCw className="h-4 w-4" /> Repeat Entries Pending
                            </span>
                          ) : hasPeriodViolations ? (
                            <span className="text-orange-700 flex items-center gap-1">
                              <AlertCircle className="h-4 w-4" /> Period Violations
                            </span>
                          ) : hasIncomplete ? (
                            <span className="text-red-700 flex items-center gap-1">
                              <XCircle className="h-4 w-4" /> Cannot Import
                            </span>
                          ) : importReadiness.totalReady > 0 ? (
                            <span className="text-green-700 flex items-center gap-1">
                              <CheckCircle className="h-4 w-4" /> Ready to Import:
                            </span>
                          ) : (
                            <span className="text-gray-600">No transactions to import</span>
                          )}
                        </span>
                        {importReadiness.totalReady > 0 && (
                          <div className="flex flex-wrap gap-2 text-xs">
                            {importReadiness.receiptsReady > 0 && (
                              <span className="bg-green-100 text-green-800 px-2 py-1 rounded">
                                {importReadiness.receiptsReady} receipt{importReadiness.receiptsReady !== 1 ? 's' : ''}
                              </span>
                            )}
                            {importReadiness.paymentsReady > 0 && (
                              <span className="bg-red-100 text-red-800 px-2 py-1 rounded">
                                {importReadiness.paymentsReady} payment{importReadiness.paymentsReady !== 1 ? 's' : ''}
                              </span>
                            )}
                            {importReadiness.refundsReady > 0 && (
                              <span className="bg-orange-100 text-orange-800 px-2 py-1 rounded">
                                {importReadiness.refundsReady} refund{importReadiness.refundsReady !== 1 ? 's' : ''}
                              </span>
                            )}
                            {importReadiness.unmatchedReady > 0 && (
                              <span className="bg-amber-100 text-amber-800 px-2 py-1 rounded">
                                {importReadiness.unmatchedReady} manually assigned
                              </span>
                            )}
                            {importReadiness.skippedReady > 0 && (
                              <span className="bg-gray-100 text-gray-800 px-2 py-1 rounded">
                                {importReadiness.skippedReady} from skipped
                              </span>
                            )}
                          </div>
                        )}
                        {hasUnhandledRepeatEntries && (
                          <span className="text-purple-600 text-xs">
                            {importReadiness.unhandledRepeatEntries} repeat entr{importReadiness.unhandledRepeatEntries !== 1 ? 'ies need' : 'y needs'} processing - update dates in Repeat Entries tab, run Opera's Recurring Entries, then re-preview
                          </span>
                        )}
                        {hasPeriodViolations && !hasUnhandledRepeatEntries && (
                          <span className="text-orange-600 text-xs">
                            {importReadiness.periodViolationsCount} transaction{importReadiness.periodViolationsCount !== 1 ? 's have dates' : ' has a date'} outside the allowed posting period - correct dates below or deselect
                          </span>
                        )}
                        {hasIncomplete && !hasPeriodViolations && !hasUnhandledRepeatEntries && (
                          <span className="text-red-600 text-xs">
                            {importReadiness.skippedIncomplete} skipped item{importReadiness.skippedIncomplete !== 1 ? 's' : ''} included but missing account - assign account or uncheck to proceed
                          </span>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              );
            })()}

            {/* Preview Results - Tabbed UI */}
            {bankPreview && (
              <div className="space-y-4">
                {/* Header with format info */}
                <div className={`p-4 rounded-lg ${bankPreview.success ? 'bg-blue-50 border border-blue-200' : 'bg-red-50 border border-red-200'}`}>
                  <div className="flex justify-between items-start mb-3">
                    <h3 className="font-semibold text-gray-900">
                      Preview: {bankPreview.filename}
                    </h3>
                    <div className="flex items-center gap-2">
                      {bankPreview.period_info && (
                        <span className="text-xs px-2 py-1 bg-purple-100 text-purple-700 rounded-full">
                          Current Period: {bankPreview.period_info.current_period}/{bankPreview.period_info.current_year}
                        </span>
                      )}
                      {bankPreview.detected_format && (
                        <span className="text-xs px-2 py-1 bg-blue-100 text-blue-700 rounded-full">
                          Format: {bankPreview.detected_format}
                        </span>
                      )}
                      <button
                        onClick={() => setShowClearStatementConfirm(true)}
                        className="text-xs px-3 py-1.5 bg-red-100 text-red-700 rounded-full hover:bg-red-200 flex items-center gap-1 font-medium border border-red-200"
                        title="Clear statement and start fresh"
                      >
                        <RotateCcw className="h-3 w-3" />
                        Clear Statement
                      </button>
                    </div>
                  </div>

                  {/* Period Violations Warning */}
                  {bankPreview.has_period_violations && bankPreview.period_violations && bankPreview.period_violations.length > 0 && (
                    <div className="mb-4 p-3 bg-orange-50 border border-orange-300 rounded-lg">
                      <div className="flex items-start gap-2">
                        <AlertCircle className="h-5 w-5 text-orange-600 flex-shrink-0 mt-0.5" />
                        <div className="flex-1">
                          <h4 className="font-medium text-orange-800">Period Validation Errors</h4>
                          <p className="text-sm text-orange-700 mt-1">
                            {bankPreview.period_violations.length} transaction{bankPreview.period_violations.length !== 1 ? 's are' : ' is'} in blocked periods.
                            {bankPreview.period_info && (
                              <span> Current period is <strong>{bankPreview.period_info.current_period}/{bankPreview.period_info.current_year}</strong>
                              {!bankPreview.period_info.open_period_accounting && <span className="text-orange-500"> (Open Period Accounting is disabled)</span>}.
                              </span>
                            )}
                          </p>
                          <div className="mt-2 text-sm text-orange-700">
                            <ul className="list-disc list-inside space-y-1">
                              {bankPreview.period_violations.slice(0, 5).map((v, idx) => (
                                <li key={idx}>
                                  <strong>{v.name || `Row ${v.row}`}</strong> ({v.date}) -
                                  {v.ledger_name && <span className="text-orange-600"> {v.ledger_name}</span>} blocked for period {v.period || v.transaction_period}/{v.year || v.transaction_year}
                                </li>
                              ))}
                              {bankPreview.period_violations.length > 5 && (
                                <li className="text-orange-500">...and {bankPreview.period_violations.length - 5} more</li>
                              )}
                            </ul>
                          </div>
                          <div className="flex items-center gap-3 mt-3">
                            <button
                              onClick={() => {
                                const today = new Date().toISOString().split('T')[0];
                                setDateOverrides(prev => {
                                  const updated = new Map(prev);
                                  bankPreview.period_violations?.forEach(v => {
                                    updated.set(v.row, today);
                                  });
                                  return updated;
                                });
                              }}
                              className="px-3 py-1.5 bg-orange-600 text-white text-sm rounded hover:bg-orange-700 flex items-center gap-1"
                            >
                              Set All to Today
                            </button>
                            <span className="text-sm text-orange-600">
                              or correct dates individually below, open the periods in Opera, or deselect these transactions
                            </span>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Tab Bar with counts and monetary values */}
                  {(() => {
                    const receipts = bankPreview.matched_receipts || [];
                    const payments = bankPreview.matched_payments || [];
                    const refunds = bankPreview.matched_refunds || [];
                    const repeatEntries = bankPreview.repeat_entries || [];
                    const unmatched = bankPreview.unmatched || [];
                    const skipped = [...(bankPreview.already_posted || []), ...(bankPreview.skipped || [])];

                    const receiptsTotal = receipts.reduce((sum, t) => sum + Math.abs(t.amount), 0);
                    const paymentsTotal = payments.reduce((sum, t) => sum + Math.abs(t.amount), 0);
                    const refundsTotal = refunds.reduce((sum, t) => sum + Math.abs(t.amount), 0);
                    const repeatTotal = repeatEntries.reduce((sum, t) => sum + Math.abs(t.amount), 0);
                    const unmatchedTotal = unmatched.reduce((sum, t) => sum + Math.abs(t.amount), 0);
                    const skippedTotal = skipped.reduce((sum, t) => sum + Math.abs(t.amount), 0);
                    const grandTotal = receiptsTotal + paymentsTotal + refundsTotal + repeatTotal + unmatchedTotal + skippedTotal;

                    return (
                      <div className="flex flex-wrap gap-2">
                        <button
                          onClick={() => { setActivePreviewTab('receipts'); setTabSearchFilter(''); }}
                          className={`flex flex-col items-center px-4 py-2 rounded-lg text-sm font-medium transition-colors min-w-[100px] ${
                            activePreviewTab === 'receipts'
                              ? 'bg-green-100 text-green-800 border-2 border-green-400'
                              : 'bg-green-50 text-green-700 border border-green-200 hover:bg-green-100'
                          }`}
                        >
                          <span className="flex items-center gap-1">Receipts <span className="bg-green-200 text-green-900 px-1.5 py-0.5 rounded-full text-xs font-bold">{receipts.length}</span></span>
                          <span className="text-sm font-bold">£{receiptsTotal.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                        </button>
                        <button
                          onClick={() => { setActivePreviewTab('payments'); setTabSearchFilter(''); }}
                          className={`flex flex-col items-center px-4 py-2 rounded-lg text-sm font-medium transition-colors min-w-[100px] ${
                            activePreviewTab === 'payments'
                              ? 'bg-red-100 text-red-800 border-2 border-red-400'
                              : 'bg-red-50 text-red-700 border border-red-200 hover:bg-red-100'
                          }`}
                        >
                          <span className="flex items-center gap-1">Payments <span className="bg-red-200 text-red-900 px-1.5 py-0.5 rounded-full text-xs font-bold">{payments.length}</span></span>
                          <span className="text-sm font-bold">£{paymentsTotal.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                        </button>
                        {refunds.length > 0 && (
                          <button
                            onClick={() => { setActivePreviewTab('refunds'); setTabSearchFilter(''); }}
                            className={`flex flex-col items-center px-4 py-2 rounded-lg text-sm font-medium transition-colors min-w-[100px] ${
                              activePreviewTab === 'refunds'
                                ? 'bg-orange-100 text-orange-800 border-2 border-orange-400'
                                : 'bg-orange-50 text-orange-700 border border-orange-200 hover:bg-orange-100'
                            }`}
                          >
                            <span className="flex items-center gap-1">Refunds <span className="bg-orange-200 text-orange-900 px-1.5 py-0.5 rounded-full text-xs font-bold">{refunds.length}</span></span>
                            <span className="text-sm font-bold">£{refundsTotal.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                          </button>
                        )}
                        {repeatEntries.length > 0 && (
                          <button
                            onClick={() => { setActivePreviewTab('repeat'); setTabSearchFilter(''); }}
                            className={`flex flex-col items-center px-4 py-2 rounded-lg text-sm font-medium transition-colors min-w-[100px] ${
                              activePreviewTab === 'repeat'
                                ? 'bg-purple-100 text-purple-800 border-2 border-purple-400'
                                : 'bg-purple-50 text-purple-700 border border-purple-200 hover:bg-purple-100'
                            }`}
                          >
                            <span className="flex items-center gap-1">Repeat <span className="bg-purple-200 text-purple-900 px-1.5 py-0.5 rounded-full text-xs font-bold">{repeatEntries.length}</span></span>
                            <span className="text-sm font-bold">£{repeatTotal.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                          </button>
                        )}
                        <button
                          onClick={() => { setActivePreviewTab('unmatched'); setTabSearchFilter(''); }}
                          className={`flex flex-col items-center px-4 py-2 rounded-lg text-sm font-medium transition-colors min-w-[100px] ${
                            activePreviewTab === 'unmatched'
                              ? 'bg-amber-100 text-amber-800 border-2 border-amber-400'
                              : 'bg-amber-50 text-amber-700 border border-amber-200 hover:bg-amber-100'
                          }`}
                        >
                          <span className="flex items-center gap-1">Unmatched <span className="bg-amber-200 text-amber-900 px-1.5 py-0.5 rounded-full text-xs font-bold">{unmatched.length}</span></span>
                          <span className="text-sm font-bold">£{unmatchedTotal.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                        </button>
                        <button
                          onClick={() => { setActivePreviewTab('skipped'); setTabSearchFilter(''); }}
                          className={`flex flex-col items-center px-4 py-2 rounded-lg text-sm font-medium transition-colors min-w-[100px] ${
                            activePreviewTab === 'skipped'
                              ? 'bg-gray-200 text-gray-800 border-2 border-gray-400'
                              : 'bg-gray-50 text-gray-600 border border-gray-200 hover:bg-gray-100'
                          }`}
                        >
                          <span className="flex items-center gap-1">Skipped <span className="bg-gray-200 text-gray-800 px-1.5 py-0.5 rounded-full text-xs font-bold">{skipped.length}</span></span>
                          <span className="text-sm font-bold">£{skippedTotal.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                        </button>
                        <div className="ml-auto flex flex-col items-center justify-center px-4 py-2 bg-blue-50 border border-blue-200 rounded-lg min-w-[120px]">
                          <span className="text-xs text-blue-600">Statement Total</span>
                          <span className="text-lg font-bold text-blue-700">£{grandTotal.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                        </div>
                      </div>
                    );
                  })()}
                </div>

                {/* Search bar for active tab */}
                {bankPreview.success && (
                  <div className="flex items-center gap-2">
                    <div className="relative flex-1">
                      <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
                      <input
                        type="text"
                        placeholder="Search by name or reference..."
                        value={tabSearchFilter}
                        onChange={(e) => setTabSearchFilter(e.target.value)}
                        className="w-full pl-9 pr-3 py-2 text-sm border border-gray-300 rounded-lg focus:ring-blue-500 focus:border-blue-500"
                      />
                      {tabSearchFilter && (
                        <button
                          onClick={() => setTabSearchFilter('')}
                          className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                        >
                          <XCircle className="h-4 w-4" />
                        </button>
                      )}
                    </div>
                  </div>
                )}

                {/* ===== RECEIPTS TAB ===== */}
                {activePreviewTab === 'receipts' && (bankPreview.matched_receipts?.length || 0) > 0 && (() => {
                  const allReceipts = bankPreview.matched_receipts || [];
                  const filtered = allReceipts.filter(txn =>
                    !tabSearchFilter || txn.name.toLowerCase().includes(tabSearchFilter.toLowerCase()) ||
                    (txn.reference || '').toLowerCase().includes(tabSearchFilter.toLowerCase())
                  );
                  const selectedCount = filtered.filter(t => selectedForImport.has(t.row)).length;
                  return (
                    <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                      <div className="flex justify-between items-center mb-2">
                        <h4 className="font-medium text-green-800">
                          Receipts to Import ({selectedCount}/{filtered.length} selected)
                        </h4>
                        <div className="flex gap-2">
                          <button
                            onClick={() => {
                              const updated = new Set(selectedForImport);
                              filtered.filter(t => !t.is_duplicate).forEach(t => updated.add(t.row));
                              setSelectedForImport(updated);
                            }}
                            className="text-xs px-2 py-1 bg-green-200 text-green-800 rounded hover:bg-green-300"
                          >
                            Select All
                          </button>
                          <button
                            onClick={() => {
                              const updated = new Set(selectedForImport);
                              filtered.forEach(t => updated.delete(t.row));
                              setSelectedForImport(updated);
                            }}
                            className="text-xs px-2 py-1 bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
                          >
                            Deselect All
                          </button>
                        </div>
                      </div>
                      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-green-100 z-10">
                            <tr>
                              <th className="p-2 w-16 text-left">Include</th>
                              <th className="text-left p-2">Date</th>
                              <th className="text-left p-2">Name</th>
                              <th className="text-left p-2">Account</th>
                              <th className="text-right p-2">Amount</th>
                              <th className="text-right p-2">Match</th>
                            </tr>
                          </thead>
                          <tbody>
                            {filtered.map((txn, idx) => (
                              <tr key={idx} className={`border-t border-green-200 ${selectedForImport.has(txn.row) ? '' : 'opacity-50'}`}>
                                <td className="p-2">
                                  <input
                                    type="checkbox"
                                    checked={selectedForImport.has(txn.row)}
                                    disabled={txn.is_duplicate}
                                    onChange={(e) => {
                                      const updated = new Set(selectedForImport);
                                      if (e.target.checked) updated.add(txn.row);
                                      else updated.delete(txn.row);
                                      setSelectedForImport(updated);
                                    }}
                                    className="rounded border-green-400"
                                    title={txn.is_duplicate ? 'Cannot import - duplicate' : ''}
                                  />
                                </td>
                                <td className="p-2">
                                  {txn.period_valid === false ? (
                                    <div className="flex items-center gap-1">
                                      <input
                                        type="date"
                                        value={dateOverrides.get(txn.row) || txn.date}
                                        onChange={(e) => {
                                          const newDate = e.target.value;
                                          setDateOverrides(prev => {
                                            const updated = new Map(prev);
                                            if (newDate && newDate !== txn.date) {
                                              updated.set(txn.row, newDate);
                                            } else {
                                              updated.delete(txn.row);
                                            }
                                            return updated;
                                          });
                                        }}
                                        className={`w-32 text-xs border rounded px-1 py-0.5 ${
                                          dateOverrides.has(txn.row) ? 'border-green-400 bg-green-50' : 'border-orange-400 bg-orange-50'
                                        }`}
                                        title={txn.period_error || 'Date outside allowed posting period'}
                                      />
                                      <button
                                        onClick={() => {
                                          const today = new Date().toISOString().split('T')[0];
                                          setDateOverrides(prev => {
                                            const updated = new Map(prev);
                                            updated.set(txn.row, today);
                                            return updated;
                                          });
                                        }}
                                        className="text-xs px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded hover:bg-blue-200"
                                        title="Set to today's date"
                                      >
                                        Today
                                      </button>
                                      {!dateOverrides.has(txn.row) && (
                                        <span title={txn.period_error || 'Date outside allowed posting period'}><AlertCircle className="h-4 w-4 text-orange-500" /></span>
                                      )}
                                      {dateOverrides.has(txn.row) && (
                                        <span title="Date corrected"><CheckCircle className="h-4 w-4 text-green-500" /></span>
                                      )}
                                    </div>
                                  ) : (
                                    txn.date
                                  )}
                                </td>
                                <td className="p-2">{txn.name}</td>
                                <td className="p-2 font-mono">{txn.account} <span className="text-gray-500 text-xs">{txn.account_name}</span></td>
                                <td className="p-2 text-right font-medium text-green-700">+£{Math.abs(txn.amount).toFixed(2)}</td>
                                <td className="p-2 text-right">{txn.match_score ? `${txn.match_score}%` : '-'}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  );
                })()}
                {activePreviewTab === 'receipts' && (bankPreview.matched_receipts?.length || 0) === 0 && (
                  <div className="text-center py-8 text-gray-500">No matched receipts found</div>
                )}

                {/* ===== PAYMENTS TAB ===== */}
                {activePreviewTab === 'payments' && (bankPreview.matched_payments?.length || 0) > 0 && (() => {
                  const allPayments = bankPreview.matched_payments || [];
                  const filtered = allPayments.filter(txn =>
                    !tabSearchFilter || txn.name.toLowerCase().includes(tabSearchFilter.toLowerCase()) ||
                    (txn.reference || '').toLowerCase().includes(tabSearchFilter.toLowerCase())
                  );
                  const selectedCount = filtered.filter(t => selectedForImport.has(t.row)).length;
                  return (
                    <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                      <div className="flex justify-between items-center mb-2">
                        <h4 className="font-medium text-red-800">
                          Payments to Import ({selectedCount}/{filtered.length} selected)
                        </h4>
                        <div className="flex gap-2">
                          <button
                            onClick={() => {
                              const updated = new Set(selectedForImport);
                              filtered.filter(t => !t.is_duplicate).forEach(t => updated.add(t.row));
                              setSelectedForImport(updated);
                            }}
                            className="text-xs px-2 py-1 bg-red-200 text-red-800 rounded hover:bg-red-300"
                          >
                            Select All
                          </button>
                          <button
                            onClick={() => {
                              const updated = new Set(selectedForImport);
                              filtered.forEach(t => updated.delete(t.row));
                              setSelectedForImport(updated);
                            }}
                            className="text-xs px-2 py-1 bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
                          >
                            Deselect All
                          </button>
                        </div>
                      </div>
                      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-red-100 z-10">
                            <tr>
                              <th className="p-2 w-16 text-left">Include</th>
                              <th className="text-left p-2">Date</th>
                              <th className="text-left p-2">Name</th>
                              <th className="text-left p-2">Account</th>
                              <th className="text-right p-2">Amount</th>
                              <th className="text-right p-2">Match</th>
                            </tr>
                          </thead>
                          <tbody>
                            {filtered.map((txn, idx) => (
                              <tr key={idx} className={`border-t border-red-200 ${selectedForImport.has(txn.row) ? '' : 'opacity-50'}`}>
                                <td className="p-2">
                                  <input
                                    type="checkbox"
                                    checked={selectedForImport.has(txn.row)}
                                    disabled={txn.is_duplicate}
                                    onChange={(e) => {
                                      const updated = new Set(selectedForImport);
                                      if (e.target.checked) updated.add(txn.row);
                                      else updated.delete(txn.row);
                                      setSelectedForImport(updated);
                                    }}
                                    className="rounded border-red-400"
                                    title={txn.is_duplicate ? 'Cannot import - duplicate' : ''}
                                  />
                                </td>
                                <td className="p-2">
                                  {txn.period_valid === false ? (
                                    <div className="flex items-center gap-1">
                                      <input
                                        type="date"
                                        value={dateOverrides.get(txn.row) || txn.date}
                                        onChange={(e) => {
                                          const newDate = e.target.value;
                                          setDateOverrides(prev => {
                                            const updated = new Map(prev);
                                            if (newDate && newDate !== txn.date) {
                                              updated.set(txn.row, newDate);
                                            } else {
                                              updated.delete(txn.row);
                                            }
                                            return updated;
                                          });
                                        }}
                                        className={`w-32 text-xs border rounded px-1 py-0.5 ${
                                          dateOverrides.has(txn.row) ? 'border-green-400 bg-green-50' : 'border-orange-400 bg-orange-50'
                                        }`}
                                        title={txn.period_error || 'Date outside allowed posting period'}
                                      />
                                      <button
                                        onClick={() => {
                                          const today = new Date().toISOString().split('T')[0];
                                          setDateOverrides(prev => {
                                            const updated = new Map(prev);
                                            updated.set(txn.row, today);
                                            return updated;
                                          });
                                        }}
                                        className="text-xs px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded hover:bg-blue-200"
                                        title="Set to today's date"
                                      >
                                        Today
                                      </button>
                                      {!dateOverrides.has(txn.row) && (
                                        <span title={txn.period_error || 'Date outside allowed posting period'}><AlertCircle className="h-4 w-4 text-orange-500" /></span>
                                      )}
                                      {dateOverrides.has(txn.row) && (
                                        <span title="Date corrected"><CheckCircle className="h-4 w-4 text-green-500" /></span>
                                      )}
                                    </div>
                                  ) : (
                                    txn.date
                                  )}
                                </td>
                                <td className="p-2">{txn.name}</td>
                                <td className="p-2 font-mono">{txn.account} <span className="text-gray-500 text-xs">{txn.account_name}</span></td>
                                <td className="p-2 text-right font-medium text-red-700">-£{Math.abs(txn.amount).toFixed(2)}</td>
                                <td className="p-2 text-right">{txn.match_score ? `${txn.match_score}%` : '-'}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  );
                })()}
                {activePreviewTab === 'payments' && (bankPreview.matched_payments?.length || 0) === 0 && (
                  <div className="text-center py-8 text-gray-500">No matched payments found</div>
                )}

                {/* ===== REFUNDS TAB ===== */}
                {activePreviewTab === 'refunds' && (() => {
                  const refunds = bankPreview.matched_refunds || [];
                  const activeRefunds = refunds.filter(txn => !refundOverrides.get(txn.row)?.rejected);
                  const rejectedCount = refunds.filter(txn => refundOverrides.get(txn.row)?.rejected).length;
                  const filtered = activeRefunds.filter(txn =>
                    !tabSearchFilter || txn.name.toLowerCase().includes(tabSearchFilter.toLowerCase()) ||
                    (txn.reference || '').toLowerCase().includes(tabSearchFilter.toLowerCase())
                  );
                  const selectedCount = filtered.filter(t => selectedForImport.has(t.row)).length;
                  if (refunds.length === 0) return <div className="text-center py-8 text-gray-500">No refunds detected</div>;
                  return (
                    <div className="bg-orange-50 border border-orange-200 rounded-lg p-4">
                      <div className="flex justify-between items-center mb-3">
                        <h4 className="font-medium text-orange-800">
                          Refunds to Import ({selectedCount}/{filtered.length} selected)
                          {rejectedCount > 0 && (
                            <span className="text-sm font-normal ml-2 text-red-600">
                              ({rejectedCount} rejected)
                            </span>
                          )}
                        </h4>
                        <div className="flex gap-2">
                          <button
                            onClick={() => {
                              const updated = new Set(selectedForImport);
                              filtered.filter(t => !t.is_duplicate).forEach(t => updated.add(t.row));
                              setSelectedForImport(updated);
                            }}
                            className="text-xs px-2 py-1 bg-orange-200 text-orange-800 rounded hover:bg-orange-300"
                          >
                            Select All
                          </button>
                          <button
                            onClick={() => {
                              const updated = new Set(selectedForImport);
                              filtered.forEach(t => updated.delete(t.row));
                              setSelectedForImport(updated);
                            }}
                            className="text-xs px-2 py-1 bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
                          >
                            Deselect All
                          </button>
                          {refundOverrides.size > 0 && (
                            <button
                              onClick={() => setRefundOverrides(new Map())}
                              className="text-xs px-2 py-1 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 flex items-center gap-1"
                            >
                              <RotateCcw className="h-3 w-3" /> Reset Changes
                            </button>
                          )}
                        </div>
                      </div>
                      <div className="text-xs text-orange-700 mb-3 bg-orange-100 p-2 rounded">
                        Review auto-detected refunds. Change type or account if needed. Uncheck to exclude from import.
                      </div>
                      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-orange-100 z-10">
                            <tr>
                              <th className="p-2 w-16 text-left">Include</th>
                              <th className="text-left p-2">Date</th>
                              <th className="text-left p-2">Name</th>
                              <th className="text-right p-2">Amount</th>
                              <th className="text-left p-2 min-w-[140px]">Type</th>
                              <th className="text-left p-2 min-w-[180px]">Account</th>
                              <th className="text-left p-2">Credit Note</th>
                            </tr>
                          </thead>
                          <tbody>
                            {filtered.map((txn) => {
                              const override = refundOverrides.get(txn.row);
                              const currentType = override?.transaction_type || txn.action as TransactionType;
                              const showCustomers = currentType === 'sales_receipt' || currentType === 'sales_refund';
                              const isNominalRef = currentType === 'nominal_receipt' || currentType === 'nominal_payment';
                              const isBankTransferRef = currentType === 'bank_transfer';
                              const currentAccount = override?.account || txn.account;
                              const isModified = override && (override.transaction_type || override.account);
                              const isSelected = selectedForImport.has(txn.row);
                              const isPositiveRef = txn.amount > 0;
                              return (
                                <tr key={txn.row} className={`border-t border-orange-200 ${isModified ? 'bg-yellow-50' : ''} ${!isSelected ? 'opacity-50' : ''}`}>
                                  <td className="p-2">
                                    <input
                                      type="checkbox"
                                      checked={isSelected}
                                      disabled={txn.is_duplicate}
                                      onChange={(e) => {
                                        const updated = new Set(selectedForImport);
                                        if (e.target.checked) updated.add(txn.row);
                                        else updated.delete(txn.row);
                                        setSelectedForImport(updated);
                                      }}
                                      className="rounded border-orange-400"
                                      title={txn.is_duplicate ? 'Cannot import - duplicate' : ''}
                                    />
                                  </td>
                                  <td className="p-2">
                                    {txn.period_valid === false ? (
                                      <div className="flex items-center gap-1">
                                        <input
                                          type="date"
                                          value={dateOverrides.get(txn.row) || txn.date}
                                          onChange={(e) => {
                                            const newDate = e.target.value;
                                            setDateOverrides(prev => {
                                              const updated = new Map(prev);
                                              if (newDate && newDate !== txn.date) {
                                                updated.set(txn.row, newDate);
                                              } else {
                                                updated.delete(txn.row);
                                              }
                                              return updated;
                                            });
                                          }}
                                          className={`w-32 text-xs border rounded px-1 py-0.5 ${
                                            dateOverrides.has(txn.row) ? 'border-green-400 bg-green-50' : 'border-orange-400 bg-orange-50'
                                          }`}
                                          title={txn.period_error || 'Date outside allowed posting period'}
                                        />
                                        <button
                                          onClick={() => {
                                            const today = new Date().toISOString().split('T')[0];
                                            setDateOverrides(prev => {
                                              const updated = new Map(prev);
                                              updated.set(txn.row, today);
                                              return updated;
                                            });
                                          }}
                                          className="text-xs px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded hover:bg-blue-200"
                                          title="Set to today's date"
                                        >
                                          Today
                                        </button>
                                        {!dateOverrides.has(txn.row) && (
                                          <span title={txn.period_error || 'Date outside allowed posting period'}><AlertCircle className="h-4 w-4 text-orange-500" /></span>
                                        )}
                                        {dateOverrides.has(txn.row) && (
                                          <span title="Date corrected"><CheckCircle className="h-4 w-4 text-green-500" /></span>
                                        )}
                                      </div>
                                    ) : (
                                      txn.date
                                    )}
                                  </td>
                                  <td className="p-2">
                                    <div className="max-w-xs truncate" title={txn.name}>{txn.name}</div>
                                  </td>
                                  <td className={`p-2 text-right font-medium ${txn.amount > 0 ? 'text-green-700' : 'text-red-700'}`}>
                                    {txn.amount > 0 ? '+' : '-'}£{Math.abs(txn.amount).toFixed(2)}
                                  </td>
                                  <td className="p-2">
                                    <select
                                      value={currentType}
                                      onChange={(e) => {
                                        const newType = e.target.value as TransactionType;
                                        if (newType === 'ignore') {
                                          openIgnoreConfirm(txn);
                                          return;
                                        }
                                        const updated = new Map(refundOverrides);
                                        const current = updated.get(txn.row) || {};
                                        const nowCustomer = newType === 'sales_receipt' || newType === 'sales_refund';
                                        const wasCustomer = currentType === 'sales_receipt' || currentType === 'sales_refund';
                                        updated.set(txn.row, {
                                          ...current,
                                          transaction_type: newType,
                                          ledger_type: nowCustomer ? 'C' : 'S',
                                          // Reset account if ledger type changed
                                          account: nowCustomer !== wasCustomer ? undefined : current.account
                                        });
                                        setRefundOverrides(updated);
                                        // Open appropriate modal for special types
                                        if (newType === 'nominal_receipt' || newType === 'nominal_payment') {
                                          openNominalDetailModal(txn, newType, 'refund');
                                        } else if (newType === 'bank_transfer') {
                                          openBankTransferModal(txn, 'refund');
                                        }
                                      }}
                                      className={`text-xs px-2 py-1 border rounded bg-white w-full ${
                                        override?.transaction_type ? 'border-yellow-400 bg-yellow-50' : 'border-gray-300'
                                      }`}
                                    >
                                      {/* Restrict based on credit/debit. Refunds are typically opposite sign */}
                                      {isPositiveRef ? (
                                        <>
                                          <option value="sales_receipt">Sales Receipt</option>
                                          <option value="purchase_refund">Purchase Refund</option>
                                          <option value="nominal_receipt">Nominal Receipt</option>
                                        </>
                                      ) : (
                                        <>
                                          <option value="purchase_payment">Purchase Payment</option>
                                          <option value="sales_refund">Sales Refund</option>
                                          <option value="nominal_payment">Nominal Payment</option>
                                        </>
                                      )}
                                      <option value="bank_transfer">Bank Transfer</option>
                                      <option value="ignore">Ignore (in Opera)</option>
                                    </select>
                                  </td>
                                  <td className="p-2">
                                    {isNominalRef ? (
                                      <div>
                                        <button
                                          onClick={() => openNominalDetailModal(txn, currentType, 'refund')}
                                          className={`w-full text-xs px-2 py-1 border rounded flex items-center justify-between ${
                                            nominalPostingDetails.has(txn.row)
                                              ? 'border-green-400 bg-green-50 text-green-700'
                                              : 'border-gray-300 bg-white text-gray-600 hover:bg-gray-50'
                                          }`}
                                        >
                                          {nominalPostingDetails.has(txn.row) ? (
                                            <>
                                              <span className="truncate">
                                                {nominalPostingDetails.get(txn.row)?.nominalCode} - £{nominalPostingDetails.get(txn.row)?.netAmount.toFixed(2)}
                                              </span>
                                              <Edit3 className="h-3 w-3 flex-shrink-0" />
                                            </>
                                          ) : (
                                            <>
                                              <span>Enter Details...</span>
                                              <Edit3 className="h-3 w-3" />
                                            </>
                                          )}
                                        </button>
                                        {nominalPostingDetails.has(txn.row) && (() => {
                                          const nominalDetail = nominalPostingDetails.get(txn.row);
                                          const nominalAcc = nominalAccounts.find(n => n.code === nominalDetail?.nominalCode);
                                          const hasVat = nominalDetail?.vatCode && nominalDetail.vatCode !== 'N/A' && nominalDetail.vatAmount > 0;
                                          return (
                                            <div className="text-xs text-gray-500 mt-1 flex items-center gap-2">
                                              <span className="truncate" title={nominalAcc?.description}>{nominalAcc?.description || 'Unknown'}</span>
                                              {hasVat && <span className="flex-shrink-0 text-green-600">+VAT</span>}
                                            </div>
                                          );
                                        })()}
                                      </div>
                                    ) : isBankTransferRef ? (
                                      <button
                                        onClick={() => openBankTransferModal(txn, 'refund')}
                                        className={`w-full text-xs px-2 py-1 border rounded flex items-center justify-between ${
                                          bankTransferDetails.has(txn.row)
                                            ? 'border-green-400 bg-green-50 text-green-700'
                                            : 'border-gray-300 bg-white text-gray-600 hover:bg-gray-50'
                                        }`}
                                      >
                                        {bankTransferDetails.has(txn.row) ? (
                                          <>
                                            <span className="truncate">
                                              {txn.amount < 0 ? 'To: ' : 'From: '}{bankTransferDetails.get(txn.row)?.destBankCode}
                                            </span>
                                            <Edit3 className="h-3 w-3 flex-shrink-0" />
                                          </>
                                        ) : (
                                          <>
                                            <span>Select Bank...</span>
                                            <Landmark className="h-3 w-3" />
                                          </>
                                        )}
                                      </button>
                                    ) : (() => {
                                      const filteredAccounts = (showCustomers ? customers : suppliers)
                                        .filter(acc => {
                                          if (!inlineAccountSearchText) return true;
                                          const search = inlineAccountSearchText.toLowerCase();
                                          return acc.code.toLowerCase().includes(search) ||
                                                 acc.name.toLowerCase().includes(search);
                                        })
                                        .slice(0, 50);
                                      return (
                                      <div className="relative">
                                        <input
                                          type="text"
                                          value={inlineAccountSearch?.row === txn.row && inlineAccountSearch?.section === 'refund'
                                            ? inlineAccountSearchText
                                            : (override?.account
                                              ? `${override.account} - ${(showCustomers ? customers : suppliers).find(a => a.code === override.account)?.name || ''}`
                                              : `${currentAccount} - ${txn.account_name || '(matched)'}`)}
                                          onChange={(e) => {
                                            setInlineAccountSearchText(e.target.value);
                                            setInlineAccountHighlightIndex(0);
                                            if (!inlineAccountSearch || inlineAccountSearch.row !== txn.row) {
                                              setInlineAccountSearch({ row: txn.row, section: 'refund' });
                                            }
                                          }}
                                          onFocus={() => {
                                            setInlineAccountSearch({ row: txn.row, section: 'refund' });
                                            setInlineAccountSearchText('');
                                            setInlineAccountHighlightIndex(0);
                                          }}
                                          onKeyDown={(e) => {
                                            // Check if this field was already filled (editing vs new)
                                            const wasAlreadyFilled = override?.account || txn.account;

                                            // Helper to move to next row's account input (only for new entries)
                                            const moveToNextRow = () => {
                                              if (wasAlreadyFilled) return; // Don't auto-advance when editing
                                              const currentIdx = filtered.findIndex(t => t.row === txn.row);
                                              if (currentIdx >= 0 && currentIdx < filtered.length - 1) {
                                                const nextRow = filtered[currentIdx + 1];
                                                setTimeout(() => {
                                                  const nextInput = document.querySelector(`[data-account-input="refund-${nextRow.row}"]`) as HTMLInputElement;
                                                  if (nextInput) nextInput.focus();
                                                }, 10);
                                              }
                                            };

                                            if (e.key === 'ArrowDown') {
                                              e.preventDefault();
                                              // Ensure dropdown is open
                                              if (!inlineAccountSearch || inlineAccountSearch.row !== txn.row) {
                                                setInlineAccountSearch({ row: txn.row, section: 'refund' });
                                              }
                                              // If only one result, select it and move to next row
                                              if (filteredAccounts.length === 1) {
                                                const selectedAcc = filteredAccounts[0];
                                                const updated = new Map(refundOverrides);
                                                const current = updated.get(txn.row) || {};
                                                updated.set(txn.row, {
                                                  ...current,
                                                  account: selectedAcc.code,
                                                  ledger_type: showCustomers ? 'C' : 'S'
                                                });
                                                setRefundOverrides(updated);
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                                moveToNextRow();
                                              } else if (filteredAccounts.length > 1) {
                                                setInlineAccountHighlightIndex(prev =>
                                                  prev < filteredAccounts.length - 1 ? prev + 1 : prev
                                                );
                                              }
                                            } else if (e.key === 'ArrowUp') {
                                              e.preventDefault();
                                              if (filteredAccounts.length > 0) {
                                                setInlineAccountHighlightIndex(prev => prev > 0 ? prev - 1 : 0);
                                              }
                                            } else if (e.key === 'Enter') {
                                              e.preventDefault();
                                              // If user hasn't typed any search text, close dropdown (don't auto-advance when editing)
                                              const userIsSearching = inlineAccountSearchText.length > 0;

                                              if (!userIsSearching) {
                                                // No search text - close dropdown, only advance if new entry
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                                if (!wasAlreadyFilled) moveToNextRow();
                                              } else if (filteredAccounts.length > 0) {
                                                // User typed search and there are results - select highlighted item
                                                const idx = Math.min(inlineAccountHighlightIndex, filteredAccounts.length - 1);
                                                const selectedAcc = filteredAccounts[idx];
                                                if (selectedAcc) {
                                                  const updated = new Map(refundOverrides);
                                                  const current = updated.get(txn.row) || {};
                                                  updated.set(txn.row, {
                                                    ...current,
                                                    account: selectedAcc.code,
                                                    ledger_type: showCustomers ? 'C' : 'S'
                                                  });
                                                  setRefundOverrides(updated);
                                                  setInlineAccountSearch(null);
                                                  setInlineAccountSearchText('');
                                                  moveToNextRow();
                                                }
                                              } else {
                                                // User typed search but no results - close dropdown
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                              }
                                            } else if (e.key === 'Escape') {
                                              setInlineAccountSearch(null);
                                              setInlineAccountSearchText('');
                                              (e.target as HTMLInputElement).blur();
                                            } else if (e.key === 'Tab') {
                                              // Tab always moves to next element (browser default), but we control the dropdown
                                              setInlineAccountSearch(null);
                                              setInlineAccountSearchText('');
                                              // Don't prevent default - let Tab work naturally
                                            }
                                          }}
                                          placeholder={`Search ${showCustomers ? 'customer' : 'supplier'}...`}
                                          data-account-input={`refund-${txn.row}`}
                                          className={`w-full text-xs px-2 py-1 border-2 rounded focus:ring-2 focus:ring-blue-500 focus:border-blue-500 focus:outline-none ${
                                            override?.account ? 'border-yellow-400 bg-yellow-50' : 'border-gray-300'
                                          }`}
                                        />
                                        {inlineAccountSearch?.row === txn.row && inlineAccountSearch?.section === 'refund' && (
                                          <>
                                            {/* Click-outside overlay - rendered first so dropdown is on top */}
                                            <div
                                              className="fixed inset-0 z-40"
                                              onClick={() => {
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                              }}
                                            />
                                            <div className="absolute z-50 w-64 mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-48 overflow-y-auto">
                                              {filteredAccounts.map((acc, idx) => (
                                                  <button
                                                    key={acc.code}
                                                    type="button"
                                                    ref={idx === inlineAccountHighlightIndex ? (el) => el?.scrollIntoView({ block: 'nearest' }) : undefined}
                                                    onClick={() => {
                                                      // Check if this was already filled (editing) vs new entry
                                                      const wasAlreadyFilled = override?.account || txn.account;
                                                      const updated = new Map(refundOverrides);
                                                      const current = updated.get(txn.row) || {};
                                                      updated.set(txn.row, {
                                                        ...current,
                                                        account: acc.code,
                                                        ledger_type: showCustomers ? 'C' : 'S'
                                                      });
                                                      setRefundOverrides(updated);
                                                      setInlineAccountSearch(null);
                                                      setInlineAccountSearchText('');
                                                      // Only move to next row if this was a new entry, not an edit
                                                      if (!wasAlreadyFilled) {
                                                        const currentIdx = filtered.findIndex(t => t.row === txn.row);
                                                        if (currentIdx >= 0 && currentIdx < filtered.length - 1) {
                                                          const nextRow = filtered[currentIdx + 1];
                                                          setTimeout(() => {
                                                            const nextInput = document.querySelector(`[data-account-input="refund-${nextRow.row}"]`) as HTMLInputElement;
                                                            if (nextInput) nextInput.focus();
                                                          }, 10);
                                                        }
                                                      }
                                                    }}
                                                    className={`w-full text-left px-2 py-1.5 text-sm ${
                                                      idx === inlineAccountHighlightIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                                                    }`}
                                                  >
                                                    <span className="font-medium">{acc.code}</span>
                                                    <span className="text-gray-600"> - {acc.name}</span>
                                                  </button>
                                                ))}
                                              {filteredAccounts.length === 0 && (
                                                <div className="px-2 py-1.5 text-sm text-gray-500">No matches found</div>
                                              )}
                                            </div>
                                          </>
                                        )}
                                      </div>
                                      );
                                    })()}
                                  </td>
                                  <td className="p-2">
                                    {txn.refund_credit_note && (
                                      <div>
                                        <span className="font-mono text-xs">{txn.refund_credit_note}</span>
                                        {txn.refund_credit_amount != null && (
                                          <span className="text-xs text-gray-500 ml-1">
                                            (£{txn.refund_credit_amount.toFixed(2)})
                                          </span>
                                        )}
                                      </div>
                                    )}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                      {refundOverrides.size > 0 && (
                        <div className="mt-3 p-3 bg-yellow-50 border border-yellow-200 rounded flex items-center gap-2 text-yellow-800">
                          <Edit3 className="h-4 w-4" />
                          <span className="text-sm">
                            {Array.from(refundOverrides.values()).filter(v => v.transaction_type || v.account).length} refund(s) modified
                          </span>
                        </div>
                      )}
                    </div>
                  );
                })()}

                {/* ===== REPEAT ENTRIES TAB ===== */}
                {activePreviewTab === 'repeat' && (() => {
                  const repeatEntries = bankPreview.repeat_entries || [];
                  const filtered = repeatEntries.filter(txn =>
                    !tabSearchFilter || txn.name.toLowerCase().includes(tabSearchFilter.toLowerCase()) ||
                    (txn.reference || '').toLowerCase().includes(tabSearchFilter.toLowerCase()) ||
                    (txn.repeat_entry_desc || '').toLowerCase().includes(tabSearchFilter.toLowerCase())
                  );
                  if (repeatEntries.length === 0) return <div className="text-center py-8 text-gray-500">No repeat entries detected</div>;
                  const handleUpdateRepeatEntryDate = async (entryRef: string, bankCode: string, newDate: string, statementName?: string, learnAlias: boolean = true) => {
                    setUpdatingRepeatEntry(entryRef);
                    try {
                      let url = `${API_BASE}/bank-import/update-repeat-entry-date?entry_ref=${encodeURIComponent(entryRef)}&bank_code=${encodeURIComponent(bankCode)}&new_date=${encodeURIComponent(newDate)}`;
                      // Include statement name for learning if user opted in
                      if (learnAlias && statementName) {
                        url += `&statement_name=${encodeURIComponent(statementName)}`;
                      }
                      const res = await authFetch(url, { method: 'POST' });
                      const data = await res.json();
                      if (data.success) {
                        setUpdatedRepeatEntries(prev => new Set(prev).add(entryRef));
                        if (data.alias_saved) {
                          console.log(`Saved alias for future matching: ${statementName} -> ${entryRef}`);
                        }
                      } else {
                        alert(`Failed to update: ${data.error}`);
                      }
                    } catch (err) {
                      alert(`Error: ${err}`);
                    } finally {
                      setUpdatingRepeatEntry(null);
                    }
                  };

                  const allUpdated = filtered.every(t => updatedRepeatEntries.has(t.repeat_entry_ref || ''));
                  const someUpdated = filtered.some(t => updatedRepeatEntries.has(t.repeat_entry_ref || ''));

                  return (
                    <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
                      <div className="flex justify-between items-center mb-3">
                        <h4 className="font-medium text-purple-800">
                          Repeat Entries ({filtered.length})
                          {someUpdated && !allUpdated && (
                            <span className="ml-2 text-sm font-normal text-purple-600">
                              ({updatedRepeatEntries.size} updated)
                            </span>
                          )}
                        </h4>
                        {allUpdated && filtered.length > 0 && (
                          <span className="text-xs bg-green-100 text-green-800 px-2 py-1 rounded flex items-center gap-1">
                            <CheckCircle className="h-3 w-3" /> All dates updated - run Opera Recurring Entries, then re-preview
                          </span>
                        )}
                      </div>

                      {/* Workflow Instructions */}
                      <div className={`text-xs mb-3 p-3 rounded ${allUpdated ? 'bg-green-100 text-green-800' : 'bg-purple-100 text-purple-800'}`}>
                        <strong>Workflow to avoid duplicates:</strong>
                        <ol className="list-decimal ml-4 mt-1 space-y-1">
                          <li className={updatedRepeatEntries.size === filtered.length ? 'line-through opacity-60' : ''}>
                            Update the Next Post Date for each entry below to match the bank statement date
                          </li>
                          <li>In Opera, go to <strong>Cashbook → Repeat Entries → Post</strong> to process these entries</li>
                          <li>Return here and click <strong>Analyse Transactions</strong> again - these will now show as "Already Posted"</li>
                          <li>Import the remaining transactions</li>
                        </ol>
                      </div>

                      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-purple-100 z-10">
                            <tr>
                              <th className="text-left p-2">Status</th>
                              <th className="text-left p-2">Statement Date</th>
                              <th className="text-left p-2">Name</th>
                              <th className="text-right p-2">Amount</th>
                              <th className="text-left p-2">Entry Ref</th>
                              <th className="text-left p-2">Description</th>
                              <th className="text-left p-2">Current Next Post</th>
                              <th className="text-left p-2">Action</th>
                            </tr>
                          </thead>
                          <tbody>
                            {filtered.map((txn) => {
                              const isUpdated = updatedRepeatEntries.has(txn.repeat_entry_ref || '');
                              const isUpdating = updatingRepeatEntry === txn.repeat_entry_ref;
                              const needsUpdate = txn.date !== txn.repeat_entry_next_date;
                              return (
                                <tr key={txn.row} className={`border-t border-purple-200 ${isUpdated ? 'bg-green-50' : txn.is_duplicate ? 'bg-amber-50' : 'hover:bg-purple-100/50'}`}>
                                  <td className="p-2">
                                    {isUpdated ? (
                                      <span className="text-green-600 flex items-center gap-1">
                                        <CheckCircle className="h-4 w-4" />
                                        <span className="text-xs">Updated</span>
                                      </span>
                                    ) : txn.is_duplicate ? (
                                      <span className="text-amber-600 flex items-center gap-1" title={txn.reason || 'Already posted'}>
                                        <AlertCircle className="h-4 w-4" />
                                        <span className="text-xs">Posted</span>
                                      </span>
                                    ) : (
                                      <span className="text-purple-600 flex items-center gap-1">
                                        <RefreshCw className="h-4 w-4" />
                                        <span className="text-xs">Pending</span>
                                      </span>
                                    )}
                                  </td>
                                  <td className="p-2 font-medium">{txn.date}</td>
                                  <td className="p-2">
                                    <div className="max-w-[150px] truncate" title={txn.name}>{txn.name}</div>
                                  </td>
                                  <td className={`p-2 text-right font-medium ${txn.amount >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                    {txn.amount >= 0 ? '+' : ''}£{Math.abs(txn.amount).toFixed(2)}
                                  </td>
                                  <td className="p-2">
                                    <span className="bg-purple-200 text-purple-800 px-2 py-0.5 rounded text-xs font-mono">
                                      {txn.repeat_entry_ref || '-'}
                                    </span>
                                  </td>
                                  <td className="p-2 text-purple-700 text-xs">{txn.repeat_entry_desc || '-'}</td>
                                  <td className="p-2">
                                    <span className={needsUpdate && !isUpdated ? 'text-orange-600' : 'text-purple-600'}>
                                      {txn.repeat_entry_next_date || '-'}
                                    </span>
                                    {needsUpdate && !isUpdated && (
                                      <span className="text-xs text-orange-500 ml-1">(differs)</span>
                                    )}
                                  </td>
                                  <td className="p-2">
                                    {(() => {
                                      // Check if entry is exhausted (posted == total && total > 0)
                                      const isExhausted = txn.repeat_entry_total && txn.repeat_entry_total > 0 &&
                                                          txn.repeat_entry_posted === txn.repeat_entry_total;
                                      // Check if date update is needed (next_post_date > statement_date)
                                      const needsDateUpdate = txn.repeat_entry_next_date && txn.date &&
                                                              txn.repeat_entry_next_date > txn.date;

                                      if (isUpdated) {
                                        return <span className="text-xs text-green-600">Done - run Opera Routine</span>;
                                      }

                                      if (isExhausted) {
                                        return (
                                          <span className="text-xs text-red-600" title={`Posted ${txn.repeat_entry_posted}/${txn.repeat_entry_total}`}>
                                            Exhausted - increase posts in Opera
                                          </span>
                                        );
                                      }

                                      if (!needsDateUpdate) {
                                        // No date update needed - next_post_date is before or equal to statement date
                                        return (
                                          <span className="text-xs text-blue-600">
                                            Run Opera Routine
                                          </span>
                                        );
                                      }

                                      // Date update needed
                                      return (
                                        <button
                                          onClick={() => {
                                            const learnAlias = window.confirm(
                                              `Update date to ${txn.date}?\n\n` +
                                              `Also remember "${txn.name}" for automatic matching in future imports?\n\n` +
                                              `(Click OK to update and remember, Cancel to update only)`
                                            );
                                            handleUpdateRepeatEntryDate(
                                              txn.repeat_entry_ref!,
                                              selectedBankCode,
                                              txn.date,
                                              txn.name,
                                              learnAlias
                                            );
                                          }}
                                          disabled={isUpdating}
                                          className="text-xs px-2 py-1 bg-purple-600 text-white rounded hover:bg-purple-700 disabled:bg-gray-400 flex items-center gap-1"
                                        >
                                          {isUpdating ? (
                                            <><Loader2 className="h-3 w-3 animate-spin" /> Updating...</>
                                          ) : (
                                            <>Update to {txn.date}</>
                                          )}
                                        </button>
                                      );
                                    })()}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  );
                })()}

                {/* ===== UNMATCHED TAB ===== */}
                {activePreviewTab === 'unmatched' && (() => {
                  const allUnmatched = bankPreview.unmatched || [];
                  const filtered = allUnmatched.filter(txn =>
                    !tabSearchFilter || txn.name.toLowerCase().includes(tabSearchFilter.toLowerCase()) ||
                    (txn.reference || '').toLowerCase().includes(tabSearchFilter.toLowerCase())
                  );
                  if (allUnmatched.length === 0) return <div className="text-center py-8 text-gray-500">No unmatched transactions</div>;
                  return (
                    <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
                      {(() => {
                        const withAccount = filtered.filter(t => editedTransactions.get(t.row)?.manual_account);
                        const selectedCount = filtered.filter(t => selectedForImport.has(t.row)).length;
                        return (
                          <div className="flex justify-between items-center mb-3">
                            <h4 className="font-medium text-amber-800">
                              Unmatched Transactions ({selectedCount}/{filtered.length} included)
                              <span className="text-sm font-normal ml-2 text-amber-600">
                                - Assign account to enable Include checkbox
                              </span>
                            </h4>
                            <div className="flex items-center gap-2">
                              {withAccount.length > 0 && (
                                <>
                                  <button
                                    onClick={() => {
                                      const updated = new Set(selectedForImport);
                                      withAccount.forEach(t => updated.add(t.row));
                                      setSelectedForImport(updated);
                                    }}
                                    className="text-xs px-2 py-1 bg-amber-200 text-amber-800 rounded hover:bg-amber-300"
                                  >
                                    Include All Assigned
                                  </button>
                                  <button
                                    onClick={() => {
                                      const updated = new Set(selectedForImport);
                                      filtered.forEach(t => updated.delete(t.row));
                                      setSelectedForImport(updated);
                                    }}
                                    className="text-xs px-2 py-1 bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
                                  >
                                    Exclude All
                                  </button>
                                </>
                              )}
                            </div>
                          </div>
                        );
                      })()}
                      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-amber-100 z-10">
                            <tr>
                              <th className="p-2 text-left w-16">Include</th>
                              <th className="text-left p-2">Date</th>
                              <th className="text-left p-2">Name</th>
                              <th className="text-right p-2">Amount</th>
                              <th className="text-left p-2">Transaction Type</th>
                              <th className="text-left p-2 min-w-[200px]">Assign Account</th>
                              <th className="text-center p-2 w-24" title="Auto-allocate to invoices after import">
                                Auto-Alloc
                                <div className="text-xs font-normal text-amber-600">(to invoice)</div>
                              </th>
                              <th className="text-left p-2">Status</th>
                            </tr>
                          </thead>
                          <tbody>
                            {filtered.map((txn) => {
                              const isIgnored = ignoredTransactions.has(txn.row);
                              const editedTxn = editedTransactions.get(txn.row);
                              const isPositive = txn.amount > 0;
                              const currentTxnType = transactionTypeOverrides.get(txn.row) || getSmartDefaultTransactionType(txn);
                              const showCustomers = currentTxnType === 'sales_receipt' || currentTxnType === 'sales_refund';
                              const isNominal = currentTxnType === 'nominal_receipt' || currentTxnType === 'nominal_payment';
                              const isBankTransfer = currentTxnType === 'bank_transfer';
                              const isNlOrTransfer = isNominal || isBankTransfer;
                              const isIncluded = selectedForImport.has(txn.row);
                              // For Nominal/Bank Transfer, account is handled elsewhere
                              const hasAccount = isNlOrTransfer || editedTxn?.manual_account;

                              // If ignored, show simplified row
                              if (isIgnored) {
                                return (
                                  <tr
                                    key={txn.row}
                                    className="border-t border-gray-200 bg-gray-100 opacity-60"
                                  >
                                    <td className="p-2">
                                      <span className="text-gray-400">-</span>
                                    </td>
                                    <td className="p-2 text-gray-500 line-through">{txn.date}</td>
                                    <td className="p-2 text-gray-500 line-through">{txn.name}</td>
                                    <td className="p-2 text-right text-gray-500 line-through">
                                      £{Math.abs(txn.amount).toFixed(2)}
                                    </td>
                                    <td colSpan={4} className="p-2 text-center">
                                      <span className="inline-flex items-center gap-1 px-2 py-1 bg-gray-200 text-gray-600 rounded text-xs">
                                        <CheckCircle className="h-3 w-3" />
                                        Ignored (already in Opera)
                                      </span>
                                    </td>
                                  </tr>
                                );
                              }

                              return (
                                <tr
                                  key={txn.row}
                                  className={`border-t border-amber-200 ${isIncluded ? 'bg-amber-100' : ''} ${editedTxn?.isEdited ? 'bg-green-50' : ''}`}
                                >
                                  <td className="p-2">
                                    <input
                                      type="checkbox"
                                      checked={isIncluded}
                                      disabled={!hasAccount}
                                      onChange={(e) => {
                                        const updated = new Set(selectedForImport);
                                        if (e.target.checked) updated.add(txn.row);
                                        else updated.delete(txn.row);
                                        setSelectedForImport(updated);
                                      }}
                                      className="rounded border-amber-400"
                                      title={!hasAccount ? 'Assign an account first to include in import' : ''}
                                    />
                                  </td>
                                  <td className="p-2">
                                    {txn.period_valid === false ? (
                                      <div className="flex items-center gap-1">
                                        <input
                                          type="date"
                                          value={dateOverrides.get(txn.row) || txn.date}
                                          onChange={(e) => {
                                            const newDate = e.target.value;
                                            setDateOverrides(prev => {
                                              const updated = new Map(prev);
                                              if (newDate && newDate !== txn.date) {
                                                updated.set(txn.row, newDate);
                                              } else {
                                                updated.delete(txn.row);
                                              }
                                              return updated;
                                            });
                                          }}
                                          className={`w-32 text-xs border rounded px-1 py-0.5 ${
                                            dateOverrides.has(txn.row) ? 'border-green-400 bg-green-50' : 'border-orange-400 bg-orange-50'
                                          }`}
                                          title={txn.period_error || 'Date outside allowed posting period'}
                                        />
                                        <button
                                          onClick={() => {
                                            const today = new Date().toISOString().split('T')[0];
                                            setDateOverrides(prev => {
                                              const updated = new Map(prev);
                                              updated.set(txn.row, today);
                                              return updated;
                                            });
                                          }}
                                          className="text-xs px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded hover:bg-blue-200"
                                          title="Set to today's date"
                                        >
                                          Today
                                        </button>
                                        {!dateOverrides.has(txn.row) && (
                                          <span title={txn.period_error || 'Date outside allowed posting period'}><AlertCircle className="h-4 w-4 text-orange-500" /></span>
                                        )}
                                        {dateOverrides.has(txn.row) && (
                                          <span title="Date corrected"><CheckCircle className="h-4 w-4 text-green-500" /></span>
                                        )}
                                      </div>
                                    ) : (
                                      txn.date
                                    )}
                                  </td>
                                  <td className="p-2">
                                    <div className="max-w-xs truncate" title={txn.name}>{txn.name}</div>
                                    {txn.reference && (
                                      <div className="text-xs text-gray-500 truncate" title={txn.reference}>Ref: {txn.reference}</div>
                                    )}
                                  </td>
                                  <td className={`p-2 text-right font-medium ${isPositive ? 'text-green-700' : 'text-red-700'}`}>
                                    {isPositive ? '+' : '-'}£{Math.abs(txn.amount).toFixed(2)}
                                  </td>
                                  <td className="p-2">
                                    <select
                                      value={currentTxnType}
                                      onChange={(e) => {
                                        const newType = e.target.value as TransactionType;
                                        if (newType === 'ignore') {
                                          openIgnoreConfirm(txn);
                                          return;
                                        }
                                        const updated = new Map(transactionTypeOverrides);
                                        updated.set(txn.row, newType);
                                        setTransactionTypeOverrides(updated);
                                        // Clear account selection if ledger type changed
                                        const wasCustomer = currentTxnType === 'sales_receipt' || currentTxnType === 'sales_refund';
                                        const nowCustomer = newType === 'sales_receipt' || newType === 'sales_refund';
                                        if (wasCustomer !== nowCustomer && editedTxn?.isEdited) {
                                          const edits = new Map(editedTransactions);
                                          edits.delete(txn.row);
                                          setEditedTransactions(edits);
                                        }
                                        // Open appropriate modal or auto-suggest
                                        if (newType === 'nominal_receipt' || newType === 'nominal_payment') {
                                          openNominalDetailModal(txn, newType, 'unmatched');
                                        } else if (newType === 'bank_transfer') {
                                          openBankTransferModal(txn, 'unmatched');
                                        } else {
                                          // Auto-suggest account for customer/supplier types
                                          suggestAccountForTransaction(txn, newType);
                                        }
                                      }}
                                      className="text-xs px-2 py-1 border border-gray-300 rounded bg-white w-full"
                                    >
                                      {/* Credit (positive): Sales Receipt, Purchase Refund, Nominal Receipt */}
                                      {/* Debit (negative): Purchase Payment, Sales Refund, Nominal Payment */}
                                      {isPositive ? (
                                        <>
                                          <option value="sales_receipt">Sales Receipt</option>
                                          <option value="purchase_refund">Purchase Refund</option>
                                          <option value="nominal_receipt">Nominal Receipt</option>
                                        </>
                                      ) : (
                                        <>
                                          <option value="purchase_payment">Purchase Payment</option>
                                          <option value="sales_refund">Sales Refund</option>
                                          <option value="nominal_payment">Nominal Payment</option>
                                        </>
                                      )}
                                      <option value="bank_transfer">Bank Transfer</option>
                                      <option value="ignore">Ignore (in Opera)</option>
                                    </select>
                                  </td>
                                  <td className="p-2">
                                    {/* Show edit button for nominal types, dropdown for others */}
                                    {isNominal ? (
                                      <div>
                                        <button
                                          onClick={() => openNominalDetailModal(txn, currentTxnType, 'unmatched')}
                                          className={`w-full text-sm px-2 py-1 border rounded flex items-center justify-between ${
                                            nominalPostingDetails.has(txn.row)
                                              ? 'border-green-400 bg-green-50 text-green-700'
                                              : 'border-gray-300 bg-white text-gray-600 hover:bg-gray-50'
                                          }`}
                                        >
                                          {nominalPostingDetails.has(txn.row) ? (
                                            <>
                                              <span className="truncate">
                                                {nominalPostingDetails.get(txn.row)?.nominalCode} - £{nominalPostingDetails.get(txn.row)?.netAmount.toFixed(2)}
                                              </span>
                                              <Edit3 className="h-3 w-3 flex-shrink-0" />
                                            </>
                                          ) : (
                                            <>
                                              <span>Enter Details...</span>
                                              <Edit3 className="h-3 w-3" />
                                            </>
                                          )}
                                        </button>
                                        {nominalPostingDetails.has(txn.row) && (() => {
                                          const nominalDetail = nominalPostingDetails.get(txn.row);
                                          const nominalAcc = nominalAccounts.find(n => n.code === nominalDetail?.nominalCode);
                                          const hasVat = nominalDetail?.vatCode && nominalDetail.vatCode !== 'N/A' && nominalDetail.vatAmount > 0;
                                          return (
                                            <div className="text-xs text-gray-500 mt-1 flex items-center gap-2">
                                              <span className="truncate" title={nominalAcc?.description}>{nominalAcc?.description || 'Unknown'}</span>
                                              {hasVat && <span className="flex-shrink-0 text-green-600">+VAT</span>}
                                            </div>
                                          );
                                        })()}
                                      </div>
                                    ) : isBankTransfer ? (
                                      <button
                                        onClick={() => openBankTransferModal(txn, 'unmatched')}
                                        className={`w-full text-sm px-2 py-1 border rounded flex items-center justify-between ${
                                          bankTransferDetails.has(txn.row)
                                            ? 'border-green-400 bg-green-50 text-green-700'
                                            : 'border-gray-300 bg-white text-gray-600 hover:bg-gray-50'
                                        }`}
                                      >
                                        {bankTransferDetails.has(txn.row) ? (
                                          <>
                                            <span className="truncate">
                                              {txn.amount < 0 ? 'To: ' : 'From: '}{bankTransferDetails.get(txn.row)?.destBankCode}
                                            </span>
                                            <Edit3 className="h-3 w-3 flex-shrink-0" />
                                          </>
                                        ) : (
                                          <>
                                            <span>Select Bank...</span>
                                            <Landmark className="h-3 w-3" />
                                          </>
                                        )}
                                      </button>
                                    ) : (() => {
                                      const filteredAccounts = (showCustomers ? customers : suppliers)
                                        .filter(acc => {
                                          if (!inlineAccountSearchText) return true;
                                          const search = inlineAccountSearchText.toLowerCase();
                                          return acc.code.toLowerCase().includes(search) ||
                                                 acc.name.toLowerCase().includes(search);
                                        })
                                        .slice(0, 50);
                                      return (
                                      <div className="relative">
                                        <input
                                          type="text"
                                          value={inlineAccountSearch?.row === txn.row && inlineAccountSearch?.section === 'unmatched'
                                            ? inlineAccountSearchText
                                            : (editedTxn?.manual_account
                                              ? `${editedTxn.manual_account} - ${editedTxn.account_name || ''}`
                                              : '')}
                                          onChange={(e) => {
                                            setInlineAccountSearchText(e.target.value);
                                            setInlineAccountHighlightIndex(0);
                                            if (!inlineAccountSearch || inlineAccountSearch.row !== txn.row) {
                                              setInlineAccountSearch({ row: txn.row, section: 'unmatched' });
                                            }
                                          }}
                                          onFocus={() => {
                                            setInlineAccountSearch({ row: txn.row, section: 'unmatched' });
                                            setInlineAccountSearchText('');
                                            setInlineAccountHighlightIndex(0);
                                          }}
                                          onKeyDown={(e) => {
                                            // Check if this field was already filled (editing vs new)
                                            const wasAlreadyFilled = editedTxn?.manual_account;

                                            // Helper to move to next row's account input (only for new entries)
                                            const moveToNextRow = () => {
                                              if (wasAlreadyFilled) return; // Don't auto-advance when editing
                                              const currentIdx = filtered.findIndex(t => t.row === txn.row);
                                              if (currentIdx >= 0 && currentIdx < filtered.length - 1) {
                                                const nextRow = filtered[currentIdx + 1];
                                                setTimeout(() => {
                                                  const nextInput = document.querySelector(`[data-account-input="unmatched-${nextRow.row}"]`) as HTMLInputElement;
                                                  if (nextInput) nextInput.focus();
                                                }, 10);
                                              }
                                            };

                                            if (e.key === 'ArrowDown') {
                                              e.preventDefault();
                                              // Ensure dropdown is open
                                              if (!inlineAccountSearch || inlineAccountSearch.row !== txn.row) {
                                                setInlineAccountSearch({ row: txn.row, section: 'unmatched' });
                                              }
                                              // If only one result, select it and move to next row
                                              if (filteredAccounts.length === 1) {
                                                const selectedAcc = filteredAccounts[0];
                                                handleAccountChange(txn, selectedAcc.code, showCustomers ? 'C' : 'S');
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                                moveToNextRow();
                                              } else if (filteredAccounts.length > 1) {
                                                setInlineAccountHighlightIndex(prev =>
                                                  prev < filteredAccounts.length - 1 ? prev + 1 : prev
                                                );
                                              }
                                            } else if (e.key === 'ArrowUp') {
                                              e.preventDefault();
                                              if (filteredAccounts.length > 0) {
                                                setInlineAccountHighlightIndex(prev => prev > 0 ? prev - 1 : 0);
                                              }
                                            } else if (e.key === 'Enter') {
                                              e.preventDefault();
                                              // If user hasn't typed any search text, just close dropdown (don't auto-advance when editing)
                                              const userIsSearching = inlineAccountSearchText.length > 0;

                                              if (!userIsSearching) {
                                                // No search text - close dropdown, only advance if new entry
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                                if (!wasAlreadyFilled) moveToNextRow();
                                              } else if (filteredAccounts.length > 0) {
                                                // User typed search and there are results - select highlighted item
                                                const idx = Math.min(inlineAccountHighlightIndex, filteredAccounts.length - 1);
                                                const selectedAcc = filteredAccounts[idx];
                                                if (selectedAcc) {
                                                  handleAccountChange(txn, selectedAcc.code, showCustomers ? 'C' : 'S');
                                                  setInlineAccountSearch(null);
                                                  setInlineAccountSearchText('');
                                                  moveToNextRow();
                                                }
                                              } else {
                                                // User typed search but no results - close dropdown
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                              }
                                            } else if (e.key === 'Escape') {
                                              setInlineAccountSearch(null);
                                              setInlineAccountSearchText('');
                                              (e.target as HTMLInputElement).blur();
                                            } else if (e.key === 'Tab') {
                                              // Tab always moves to next element (browser default), but we control the dropdown
                                              setInlineAccountSearch(null);
                                              setInlineAccountSearchText('');
                                              // Don't prevent default - let Tab work naturally
                                            }
                                          }}
                                          placeholder={`Search ${showCustomers ? 'customer' : 'supplier'}...`}
                                          data-account-input={`unmatched-${txn.row}`}
                                          className={`w-full text-sm px-2 py-1 border-2 rounded focus:ring-2 focus:ring-blue-500 focus:border-blue-500 focus:outline-none ${
                                            editedTxn?.isEdited ? 'border-green-400 bg-green-50' : 'border-gray-300'
                                          }`}
                                        />
                                        {inlineAccountSearch?.row === txn.row && inlineAccountSearch?.section === 'unmatched' && (
                                          <>
                                            {/* Click-outside overlay - rendered first so dropdown is on top */}
                                            <div
                                              className="fixed inset-0 z-40"
                                              onClick={() => {
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                              }}
                                            />
                                            <div className="absolute z-50 w-64 mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-48 overflow-y-auto">
                                              {filteredAccounts.map((acc, idx) => (
                                                  <button
                                                    key={acc.code}
                                                    type="button"
                                                    ref={idx === inlineAccountHighlightIndex ? (el) => el?.scrollIntoView({ block: 'nearest' }) : undefined}
                                                    onClick={() => {
                                                      // Check if this was already filled (editing) vs new entry
                                                      const wasAlreadyFilled = editedTxn?.manual_account;
                                                      handleAccountChange(txn, acc.code, showCustomers ? 'C' : 'S');
                                                      setInlineAccountSearch(null);
                                                      setInlineAccountSearchText('');
                                                      // Only move to next row if this was a new entry, not an edit
                                                      if (!wasAlreadyFilled) {
                                                        const currentIdx = filtered.findIndex(t => t.row === txn.row);
                                                        if (currentIdx >= 0 && currentIdx < filtered.length - 1) {
                                                          const nextRow = filtered[currentIdx + 1];
                                                          setTimeout(() => {
                                                            const nextInput = document.querySelector(`[data-account-input="unmatched-${nextRow.row}"]`) as HTMLInputElement;
                                                            if (nextInput) nextInput.focus();
                                                          }, 10);
                                                        }
                                                      }
                                                    }}
                                                    className={`w-full text-left px-2 py-1.5 text-sm ${
                                                      idx === inlineAccountHighlightIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                                                    }`}
                                                  >
                                                    <span className="font-medium">{acc.code}</span>
                                                    <span className="text-gray-600"> - {acc.name}</span>
                                                  </button>
                                                ))}
                                              {filteredAccounts.length === 0 && (
                                                <div className="px-2 py-1.5 text-sm text-gray-500">No matches found</div>
                                              )}
                                            </div>
                                          </>
                                        )}
                                      </div>
                                      );
                                    })()}
                                  </td>
                                  {/* Auto-Allocate checkbox - defaults checked unless explicitly disabled */}
                                  <td className="p-2 text-center">
                                    {(() => {
                                      // Only show for customer/supplier transaction types (not nominal or bank transfer)
                                      const canAutoAllocate = currentTxnType === 'sales_receipt' || currentTxnType === 'purchase_payment' ||
                                                             currentTxnType === 'sales_refund' || currentTxnType === 'purchase_refund';
                                      const rowAutoAllocEnabled = !autoAllocateDisabled.has(txn.row);

                                      if (!canAutoAllocate) {
                                        return <span className="text-gray-400 text-xs">N/A</span>;
                                      }

                                      if (!hasAccount) {
                                        return <span className="text-gray-400 text-xs">-</span>;
                                      }

                                      return (
                                        <input
                                          type="checkbox"
                                          checked={rowAutoAllocEnabled}
                                          onChange={(e) => {
                                            const updated = new Set(autoAllocateDisabled);
                                            if (e.target.checked) {
                                              // Enable auto-allocate (remove from disabled set)
                                              updated.delete(txn.row);
                                            } else {
                                              // Disable auto-allocate for this row
                                              updated.add(txn.row);
                                            }
                                            setAutoAllocateDisabled(updated);
                                          }}
                                          className="rounded border-green-400 text-green-600 focus:ring-green-500"
                                          title={rowAutoAllocEnabled ? 'Auto-allocate to invoices' : 'Skip auto-allocation (post on account)'}
                                        />
                                      );
                                    })()}
                                  </td>
                                  <td className="p-2">
                                    {(editedTxn?.isEdited || nominalPostingDetails.has(txn.row) || bankTransferDetails.has(txn.row)) ? (
                                      <span className="inline-flex items-center gap-1 text-green-600 text-xs">
                                        <CheckCircle className="h-3 w-3" />
                                        {(txn as any).suggested_account ? 'Auto-filled' : 'Ready'}
                                      </span>
                                    ) : txn.is_duplicate ? (
                                      <span className="inline-flex items-center gap-1 text-orange-600 text-xs" title="Potential duplicate detected">
                                        <AlertCircle className="h-3 w-3" /> Duplicate?
                                      </span>
                                    ) : (
                                      <span className="text-gray-400 text-xs">Unassigned</span>
                                    )}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                      {editedTransactions.size > 0 && (
                        <div className="mt-3 p-3 bg-green-50 border border-green-200 rounded flex items-center justify-between">
                          <div className="flex items-center gap-2 text-green-700">
                            <Edit3 className="h-4 w-4" />
                            <span className="text-sm font-medium">
                              {editedTransactions.size} transaction(s) assigned and ready to import
                            </span>
                          </div>
                          <button
                            onClick={() => { setEditedTransactions(new Map()); setTransactionTypeOverrides(new Map()); }}
                            className="text-sm text-green-600 hover:text-green-800 flex items-center gap-1"
                          >
                            <RefreshCw className="h-3 w-3" /> Reset All
                          </button>
                        </div>
                      )}
                    </div>
                  );
                })()}

                {/* ===== SKIPPED TAB ===== */}
                {activePreviewTab === 'skipped' && (() => {
                  const allSkipped = [...(bankPreview.already_posted || []), ...(bankPreview.skipped || [])];
                  const filtered = allSkipped.filter(txn =>
                    !tabSearchFilter || txn.name.toLowerCase().includes(tabSearchFilter.toLowerCase()) ||
                    (txn.reference || '').toLowerCase().includes(tabSearchFilter.toLowerCase())
                  );
                  if (allSkipped.length === 0) return <div className="text-center py-8 text-gray-500">No skipped transactions</div>;
                  return (
                    <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
                      <div className="flex justify-between items-center mb-3">
                        <h4 className="font-medium text-gray-800">
                          Skipped ({filtered.length})
                          <span className="text-sm font-normal ml-2 text-gray-500">
                            - Check items and assign type + account to include in import
                          </span>
                        </h4>
                        {includedSkipped.size > 0 && (
                          <button
                            onClick={() => setIncludedSkipped(new Map())}
                            className="text-sm text-gray-600 hover:text-gray-800 flex items-center gap-1"
                          >
                            <RotateCcw className="h-3 w-3" /> Clear Inclusions
                          </button>
                        )}
                      </div>
                      <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-gray-100 z-10">
                            <tr>
                              <th className="p-2 text-left w-8">Include</th>
                              <th className="text-left p-2">Date</th>
                              <th className="text-left p-2">Name</th>
                              <th className="text-right p-2">Amount</th>
                              <th className="text-left p-2">Reason</th>
                              <th className="text-left p-2">Transaction Type</th>
                              <th className="text-left p-2 min-w-[180px]">Assign Account</th>
                            </tr>
                          </thead>
                          <tbody>
                            {filtered.map((txn, idx) => {
                              const isIncluded = includedSkipped.has(txn.row);
                              const inclusion = includedSkipped.get(txn.row);
                              const isAlreadyPosted = txn.is_duplicate || (txn.reason && txn.reason.includes('Already'));
                              const isPositive = txn.amount > 0;
                              const skippedTxnType = inclusion?.transaction_type || getSmartDefaultTransactionType(txn);
                              const showCust = skippedTxnType === 'sales_receipt' || skippedTxnType === 'sales_refund';
                              const isNominalSkip = skippedTxnType === 'nominal_receipt' || skippedTxnType === 'nominal_payment';
                              const isBankTransferSkip = skippedTxnType === 'bank_transfer';
                              return (
                                <tr key={idx} className={`border-t border-gray-200 ${isIncluded ? 'bg-green-50' : ''}`}>
                                  <td className="p-2">
                                    {!isAlreadyPosted && (
                                      <input
                                        type="checkbox"
                                        checked={isIncluded}
                                        onChange={(e) => {
                                          const updated = new Map(includedSkipped);
                                          if (e.target.checked) {
                                            const smartType = getSmartDefaultTransactionType(txn);
                                            const isCustomerType = smartType === 'sales_receipt' || smartType === 'sales_refund';
                                            updated.set(txn.row, {
                                              account: '',
                                              ledger_type: isCustomerType ? 'C' : 'S',
                                              transaction_type: smartType
                                            });
                                          } else {
                                            updated.delete(txn.row);
                                          }
                                          setIncludedSkipped(updated);
                                        }}
                                        className="rounded border-gray-400"
                                      />
                                    )}
                                  </td>
                                  <td className="p-2">
                                    {txn.period_valid === false && isIncluded ? (
                                      <div className="flex items-center gap-1">
                                        <input
                                          type="date"
                                          value={dateOverrides.get(txn.row) || txn.date}
                                          onChange={(e) => {
                                            const newDate = e.target.value;
                                            setDateOverrides(prev => {
                                              const updated = new Map(prev);
                                              if (newDate && newDate !== txn.date) {
                                                updated.set(txn.row, newDate);
                                              } else {
                                                updated.delete(txn.row);
                                              }
                                              return updated;
                                            });
                                          }}
                                          className={`w-32 text-xs border rounded px-1 py-0.5 ${
                                            dateOverrides.has(txn.row) ? 'border-green-400 bg-green-50' : 'border-orange-400 bg-orange-50'
                                          }`}
                                          title={txn.period_error || 'Date outside allowed posting period'}
                                        />
                                        <button
                                          onClick={() => {
                                            const today = new Date().toISOString().split('T')[0];
                                            setDateOverrides(prev => {
                                              const updated = new Map(prev);
                                              updated.set(txn.row, today);
                                              return updated;
                                            });
                                          }}
                                          className="text-xs px-1.5 py-0.5 bg-blue-100 text-blue-700 rounded hover:bg-blue-200"
                                          title="Set to today's date"
                                        >
                                          Today
                                        </button>
                                        {!dateOverrides.has(txn.row) && (
                                          <span title={txn.period_error || 'Date outside allowed posting period'}><AlertCircle className="h-4 w-4 text-orange-500" /></span>
                                        )}
                                        {dateOverrides.has(txn.row) && (
                                          <span title="Date corrected"><CheckCircle className="h-4 w-4 text-green-500" /></span>
                                        )}
                                      </div>
                                    ) : (
                                      <span className={txn.period_valid === false ? 'text-orange-600' : ''}>
                                        {txn.date}
                                        {txn.period_valid === false && !isIncluded && (
                                          <span title={txn.period_error || 'Date outside allowed posting period'}><AlertCircle className="inline h-3 w-3 ml-1 text-orange-500" /></span>
                                        )}
                                      </span>
                                    )}
                                  </td>
                                  <td className="p-2">
                                    <div className="max-w-xs truncate" title={txn.name}>{txn.name}</div>
                                  </td>
                                  <td className={`p-2 text-right font-medium ${isPositive ? 'text-green-700' : 'text-red-700'}`}>
                                    {isPositive ? '+' : '-'}£{Math.abs(txn.amount).toFixed(2)}
                                  </td>
                                  <td className="p-2 text-gray-600 text-xs max-w-xs truncate" title={txn.reason || 'Already posted'}>
                                    {txn.reason || 'Already posted'}
                                  </td>
                                  <td className="p-2">
                                    {isIncluded && (
                                      <select
                                        value={skippedTxnType}
                                        onChange={(e) => {
                                          const newType = e.target.value as TransactionType;
                                          if (newType === 'ignore') {
                                            openIgnoreConfirm(txn);
                                            return;
                                          }
                                          const updated = new Map(includedSkipped);
                                          const current = updated.get(txn.row)!;
                                          const nowCustomer = newType === 'sales_receipt' || newType === 'sales_refund';
                                          updated.set(txn.row, {
                                            ...current,
                                            transaction_type: newType,
                                            ledger_type: nowCustomer ? 'C' : 'S',
                                            account: '' // Reset account on type change
                                          });
                                          setIncludedSkipped(updated);
                                          // Open appropriate modal for special types
                                          if (newType === 'nominal_receipt' || newType === 'nominal_payment') {
                                            openNominalDetailModal(txn, newType, 'skipped');
                                          } else if (newType === 'bank_transfer') {
                                            openBankTransferModal(txn, 'skipped');
                                          }
                                        }}
                                        className="text-xs px-2 py-1 border border-gray-300 rounded bg-white w-full"
                                      >
                                        {/* Restrict based on credit/debit */}
                                        {isPositive ? (
                                          <>
                                            <option value="sales_receipt">Sales Receipt</option>
                                            <option value="purchase_refund">Purchase Refund</option>
                                            <option value="nominal_receipt">Nominal Receipt</option>
                                          </>
                                        ) : (
                                          <>
                                            <option value="purchase_payment">Purchase Payment</option>
                                            <option value="sales_refund">Sales Refund</option>
                                            <option value="nominal_payment">Nominal Payment</option>
                                          </>
                                        )}
                                        <option value="bank_transfer">Bank Transfer</option>
                                        <option value="ignore">Ignore (in Opera)</option>
                                      </select>
                                    )}
                                  </td>
                                  <td className="p-2">
                                    {isIncluded ? (
                                      isNominalSkip ? (
                                        <div>
                                          <button
                                            onClick={() => openNominalDetailModal(txn, skippedTxnType, 'skipped')}
                                            className={`w-full text-sm px-2 py-1 border rounded flex items-center justify-between ${
                                              nominalPostingDetails.has(txn.row)
                                                ? 'border-green-400 bg-green-50 text-green-700'
                                                : 'border-gray-300 bg-white text-gray-600 hover:bg-gray-50'
                                            }`}
                                          >
                                            {nominalPostingDetails.has(txn.row) ? (
                                              <>
                                                <span className="truncate">
                                                  {nominalPostingDetails.get(txn.row)?.nominalCode} - £{nominalPostingDetails.get(txn.row)?.netAmount.toFixed(2)}
                                                </span>
                                                <Edit3 className="h-3 w-3 flex-shrink-0" />
                                              </>
                                            ) : (
                                              <>
                                                <span>Enter Details...</span>
                                                <Edit3 className="h-3 w-3" />
                                              </>
                                            )}
                                          </button>
                                          {nominalPostingDetails.has(txn.row) && (() => {
                                            const nominalDetail = nominalPostingDetails.get(txn.row);
                                            const nominalAcc = nominalAccounts.find(n => n.code === nominalDetail?.nominalCode);
                                            const hasVat = nominalDetail?.vatCode && nominalDetail.vatCode !== 'N/A' && nominalDetail.vatAmount > 0;
                                            return (
                                              <div className="text-xs text-gray-500 mt-1 flex items-center gap-2">
                                                <span className="truncate" title={nominalAcc?.description}>{nominalAcc?.description || 'Unknown'}</span>
                                                {hasVat && <span className="flex-shrink-0 text-green-600">+VAT</span>}
                                              </div>
                                            );
                                          })()}
                                        </div>
                                      ) : isBankTransferSkip ? (
                                        <button
                                          onClick={() => openBankTransferModal(txn, 'skipped')}
                                          className={`w-full text-sm px-2 py-1 border rounded flex items-center justify-between ${
                                            bankTransferDetails.has(txn.row)
                                              ? 'border-green-400 bg-green-50 text-green-700'
                                              : 'border-gray-300 bg-white text-gray-600 hover:bg-gray-50'
                                          }`}
                                        >
                                          {bankTransferDetails.has(txn.row) ? (
                                            <>
                                              <span className="truncate">
                                                {txn.amount < 0 ? 'To: ' : 'From: '}{bankTransferDetails.get(txn.row)?.destBankCode}
                                              </span>
                                              <Edit3 className="h-3 w-3 flex-shrink-0" />
                                            </>
                                          ) : (
                                            <>
                                              <span>Select Bank...</span>
                                              <Landmark className="h-3 w-3" />
                                            </>
                                          )}
                                        </button>
                                      ) : (() => {
                                        const filteredSkippedAccounts = (showCust ? customers : suppliers)
                                          .filter(acc => {
                                            if (!inlineAccountSearchText) return true;
                                            const search = inlineAccountSearchText.toLowerCase();
                                            return acc.code.toLowerCase().includes(search) ||
                                                   acc.name.toLowerCase().includes(search);
                                          })
                                          .slice(0, 50);
                                        return (
                                        <div className="relative">
                                          <input
                                            type="text"
                                            value={inlineAccountSearch?.row === txn.row && inlineAccountSearch?.section === 'skipped'
                                              ? inlineAccountSearchText
                                              : (inclusion?.account
                                                ? `${inclusion.account} - ${(showCust ? customers : suppliers).find(a => a.code === inclusion.account)?.name || ''}`
                                                : '')}
                                            onChange={(e) => {
                                              setInlineAccountSearchText(e.target.value);
                                              setInlineAccountHighlightIndex(0);
                                              if (!inlineAccountSearch || inlineAccountSearch.row !== txn.row) {
                                                setInlineAccountSearch({ row: txn.row, section: 'skipped' });
                                              }
                                            }}
                                            onFocus={() => {
                                              setInlineAccountSearch({ row: txn.row, section: 'skipped' });
                                              setInlineAccountSearchText('');
                                              setInlineAccountHighlightIndex(0);
                                            }}
                                            onKeyDown={(e) => {
                                              // Check if this field was already filled (editing vs new)
                                              const wasAlreadyFilled = inclusion?.account;

                                              // Helper to move to next row's account input (only for new entries)
                                              const moveToNextRow = () => {
                                                if (wasAlreadyFilled) return; // Don't auto-advance when editing
                                                const currentIdx = filtered.findIndex(t => t.row === txn.row);
                                                if (currentIdx >= 0 && currentIdx < filtered.length - 1) {
                                                  const nextRow = filtered[currentIdx + 1];
                                                  setTimeout(() => {
                                                    const nextInput = document.querySelector(`[data-account-input="skipped-${nextRow.row}"]`) as HTMLInputElement;
                                                    if (nextInput) nextInput.focus();
                                                  }, 10);
                                                }
                                              };

                                              if (e.key === 'ArrowDown') {
                                                e.preventDefault();
                                                // Ensure dropdown is open
                                                if (!inlineAccountSearch || inlineAccountSearch.row !== txn.row) {
                                                  setInlineAccountSearch({ row: txn.row, section: 'skipped' });
                                                }
                                                // If only one result, select it and move to next row
                                                if (filteredSkippedAccounts.length === 1) {
                                                  const selectedAcc = filteredSkippedAccounts[0];
                                                  const updated = new Map(includedSkipped);
                                                  const current = updated.get(txn.row)!;
                                                  updated.set(txn.row, { ...current, account: selectedAcc.code, ledger_type: showCust ? 'C' : 'S' });
                                                  setIncludedSkipped(updated);
                                                  setInlineAccountSearch(null);
                                                  setInlineAccountSearchText('');
                                                  moveToNextRow();
                                                } else if (filteredSkippedAccounts.length > 1) {
                                                  setInlineAccountHighlightIndex(prev =>
                                                    prev < filteredSkippedAccounts.length - 1 ? prev + 1 : prev
                                                  );
                                                }
                                              } else if (e.key === 'ArrowUp') {
                                                e.preventDefault();
                                                if (filteredSkippedAccounts.length > 0) {
                                                  setInlineAccountHighlightIndex(prev => prev > 0 ? prev - 1 : 0);
                                                }
                                              } else if (e.key === 'Enter') {
                                                e.preventDefault();
                                                // If user hasn't typed any search text, close dropdown (don't auto-advance when editing)
                                                const userIsSearching = inlineAccountSearchText.length > 0;

                                                if (!userIsSearching) {
                                                  // No search text - close dropdown, only advance if new entry
                                                  setInlineAccountSearch(null);
                                                  setInlineAccountSearchText('');
                                                  if (!wasAlreadyFilled) moveToNextRow();
                                                } else if (filteredSkippedAccounts.length > 0) {
                                                  // User typed search and there are results - select highlighted item
                                                  const idx = Math.min(inlineAccountHighlightIndex, filteredSkippedAccounts.length - 1);
                                                  const selectedAcc = filteredSkippedAccounts[idx];
                                                  if (selectedAcc) {
                                                    const updated = new Map(includedSkipped);
                                                    const current = updated.get(txn.row)!;
                                                    updated.set(txn.row, { ...current, account: selectedAcc.code, ledger_type: showCust ? 'C' : 'S' });
                                                    setIncludedSkipped(updated);
                                                    setInlineAccountSearch(null);
                                                    setInlineAccountSearchText('');
                                                    moveToNextRow();
                                                  }
                                                } else {
                                                  // User typed search but no results - close dropdown
                                                  setInlineAccountSearch(null);
                                                  setInlineAccountSearchText('');
                                                }
                                              } else if (e.key === 'Escape') {
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                                (e.target as HTMLInputElement).blur();
                                              } else if (e.key === 'Tab') {
                                                // Tab always moves to next element (browser default), but we control the dropdown
                                                setInlineAccountSearch(null);
                                                setInlineAccountSearchText('');
                                                // Don't prevent default - let Tab work naturally
                                              }
                                            }}
                                            placeholder={`Search ${showCust ? 'customer' : 'supplier'}...`}
                                            data-account-input={`skipped-${txn.row}`}
                                            className={`w-full text-sm px-2 py-1 border-2 rounded focus:ring-2 focus:ring-blue-500 focus:border-blue-500 focus:outline-none ${
                                              inclusion?.account ? 'border-green-400 bg-green-50' : 'border-gray-300'
                                            }`}
                                          />
                                          {inlineAccountSearch?.row === txn.row && inlineAccountSearch?.section === 'skipped' && (
                                            <>
                                              {/* Click-outside overlay - rendered first so dropdown is on top */}
                                              <div
                                                className="fixed inset-0 z-40"
                                                onClick={() => {
                                                  setInlineAccountSearch(null);
                                                  setInlineAccountSearchText('');
                                                }}
                                              />
                                              <div className="absolute z-50 w-64 mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-48 overflow-y-auto">
                                                {filteredSkippedAccounts.map((acc, idx) => (
                                                    <button
                                                      key={acc.code}
                                                      type="button"
                                                      ref={idx === inlineAccountHighlightIndex ? (el) => el?.scrollIntoView({ block: 'nearest' }) : undefined}
                                                      onClick={() => {
                                                        // Check if this was already filled (editing) vs new entry
                                                        const wasAlreadyFilled = inclusion?.account;
                                                        const updated = new Map(includedSkipped);
                                                        const current = updated.get(txn.row)!;
                                                        updated.set(txn.row, { ...current, account: acc.code, ledger_type: showCust ? 'C' : 'S' });
                                                        setIncludedSkipped(updated);
                                                        setInlineAccountSearch(null);
                                                        setInlineAccountSearchText('');
                                                        // Only move to next row if this was a new entry, not an edit
                                                        if (!wasAlreadyFilled) {
                                                          const currentIdx = filtered.findIndex(t => t.row === txn.row);
                                                          if (currentIdx >= 0 && currentIdx < filtered.length - 1) {
                                                            const nextRow = filtered[currentIdx + 1];
                                                            setTimeout(() => {
                                                              const nextInput = document.querySelector(`[data-account-input="skipped-${nextRow.row}"]`) as HTMLInputElement;
                                                              if (nextInput) nextInput.focus();
                                                            }, 10);
                                                          }
                                                        }
                                                      }}
                                                      className={`w-full text-left px-2 py-1.5 text-sm ${
                                                        idx === inlineAccountHighlightIndex ? 'bg-blue-100' : 'hover:bg-blue-50'
                                                      }`}
                                                    >
                                                      <span className="font-medium">{acc.code}</span>
                                                      <span className="text-gray-600"> - {acc.name}</span>
                                                    </button>
                                                  ))}
                                                {filteredSkippedAccounts.length === 0 && (
                                                  <div className="px-2 py-1.5 text-sm text-gray-500">No matches found</div>
                                                )}
                                              </div>
                                            </>
                                          )}
                                        </div>
                                        );
                                      })()
                                    ) : null}
                                  </td>
                                </tr>
                              );
                            })}
                          </tbody>
                        </table>
                      </div>
                      {includedSkipped.size > 0 && (
                        <div className="mt-3 p-3 bg-green-50 border border-green-200 rounded flex items-center gap-2 text-green-700">
                          <Edit3 className="h-4 w-4" />
                          <span className="text-sm font-medium">
                            {includedSkipped.size} skipped item(s) included for import
                            {Array.from(includedSkipped.values()).filter(v => !v.account).length > 0 && (
                              <span className="text-amber-600 ml-2">
                                ({Array.from(includedSkipped.values()).filter(v => !v.account).length} still need account assignment)
                              </span>
                            )}
                          </span>
                        </div>
                      )}
                    </div>
                  );
                })()}

                {/* Step 4: Import Section - appears after reviewing transactions */}
                <div className="mt-6 p-4 bg-gray-50 border border-gray-200 rounded-lg">
                  <h3 className="text-lg font-semibold text-gray-800 mb-4 flex items-center gap-2">
                    <CheckCircle className="h-5 w-5 text-green-600" />
                    Step 4: Import to Opera
                  </h3>

                  {/* Import Readiness Summary */}
                  <div className={`p-3 rounded-lg border mb-4 ${
                    importReadiness?.canImport
                      ? 'bg-green-50 border-green-200'
                      : 'bg-amber-50 border-amber-200'
                  }`}>
                    <div className="flex items-start gap-3">
                      {importReadiness?.canImport ? (
                        <CheckCircle className="h-5 w-5 text-green-600 mt-0.5 flex-shrink-0" />
                      ) : (
                        <AlertCircle className="h-5 w-5 text-amber-600 mt-0.5 flex-shrink-0" />
                      )}
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-4 flex-wrap text-sm">
                          <span className={importReadiness?.canImport ? 'text-green-800 font-medium' : 'text-amber-800 font-medium'}>
                            {importReadiness?.canImport
                              ? `Ready to import ${importReadiness.totalReady} transaction${importReadiness.totalReady !== 1 ? 's' : ''}`
                              : 'Import blocked - action required'}
                          </span>
                          {/* Show breakdown */}
                          {importReadiness && (
                            <span className="text-gray-600 text-xs">
                              {importReadiness.receiptsReady > 0 && <span className="mr-2">✓ {importReadiness.receiptsReady} receipts</span>}
                              {importReadiness.paymentsReady > 0 && <span className="mr-2">✓ {importReadiness.paymentsReady} payments</span>}
                              {importReadiness.refundsReady > 0 && <span className="mr-2">✓ {importReadiness.refundsReady} refunds</span>}
                              {importReadiness.unmatchedReady > 0 && <span className="mr-2">✓ {importReadiness.unmatchedReady} unmatched</span>}
                              {importReadiness.skippedReady > 0 && <span className="mr-2">✓ {importReadiness.skippedReady} included</span>}
                            </span>
                          )}
                        </div>
                        {/* Show issues if any */}
                        {importReadiness && !importReadiness.canImport && (
                          <div className="mt-2 text-sm text-amber-700 space-y-2">
                            {importReadiness.totalIncomplete > 0 && (
                              <div className="p-3 bg-amber-100 rounded">
                                <div className="flex items-center gap-1 font-medium">
                                  <XCircle className="h-3.5 w-3.5" />
                                  <span>{importReadiness.totalIncomplete} transaction{importReadiness.totalIncomplete !== 1 ? 's' : ''} missing account assignment</span>
                                </div>
                                <div className="mt-2 ml-4 text-xs text-amber-700 space-y-1">
                                  <p><strong>Option 1:</strong> Assign an account in the Unmatched tab above (e.g., select Customer/Supplier)</p>
                                  <p><strong>Option 2:</strong> Exclude from import and enter manually in Opera:</p>
                                  <ul className="list-disc list-inside ml-4 text-amber-600">
                                    <li>Uncheck the transaction above to exclude it</li>
                                    <li>Enter the receipt/payment directly in Opera Cashbook</li>
                                    <li>Note: Excluded items won't appear in auto-reconcile - you'll mark them off manually</li>
                                  </ul>
                                </div>
                                <button
                                  type="button"
                                  onClick={() => {
                                    // Find and deselect all unmatched transactions without accounts
                                    const unmatched = bankPreview?.unmatched || [];
                                    const newSelected = new Set(selectedForImport);
                                    unmatched.forEach(txn => {
                                      const edited = editedTransactions.get(txn.row);
                                      if (!edited?.manual_account && selectedForImport.has(txn.row)) {
                                        newSelected.delete(txn.row);
                                      }
                                    });
                                    // Also check skipped included items
                                    includedSkipped.forEach((data, row) => {
                                      if (!data.account) {
                                        setIncludedSkipped(prev => {
                                          const updated = new Map(prev);
                                          updated.delete(row);
                                          return updated;
                                        });
                                      }
                                    });
                                    setSelectedForImport(newSelected);
                                  }}
                                  className="mt-3 ml-4 px-3 py-1.5 text-xs bg-amber-200 hover:bg-amber-300 text-amber-800 rounded transition-colors font-medium"
                                >
                                  Exclude all unassigned (enter in Opera manually)
                                </button>
                              </div>
                            )}
                            {importReadiness.hasPeriodViolations && (
                              <div className="flex items-center gap-1">
                                <XCircle className="h-3.5 w-3.5" />
                                <span>{importReadiness.periodViolationsCount} transaction{importReadiness.periodViolationsCount !== 1 ? 's have' : ' has'} dates outside allowed posting period</span>
                              </div>
                            )}
                            {importReadiness.hasUnhandledRepeatEntries && (
                              <div className="flex items-center gap-1">
                                <XCircle className="h-3.5 w-3.5" />
                                <span>{importReadiness.unhandledRepeatEntries} repeat entr{importReadiness.unhandledRepeatEntries !== 1 ? 'ies need' : 'y needs'} processing in Opera first</span>
                              </div>
                            )}
                            {importReadiness.totalReady === 0 && importReadiness.totalIncomplete === 0 && (
                              <div className="flex items-center gap-1">
                                <XCircle className="h-3.5 w-3.5" />
                                <span>No transactions selected for import - check the boxes to include items</span>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    </div>
                  </div>

                  {/* Import Controls */}
                  <div className="flex items-center gap-4 flex-wrap">
                    {/* Import Button */}
                    <button
                      onClick={isEmailSource ? handleEmailImport : isPdfSource ? handlePdfImport : handleBankImport}
                      disabled={importDisabled || isImporting}
                      className={`px-6 py-3 rounded-lg flex items-center gap-2 font-medium text-lg ${
                        importDisabled || isImporting
                          ? 'bg-gray-400 text-white cursor-not-allowed'
                          : 'bg-green-600 text-white hover:bg-green-700 shadow-md hover:shadow-lg transition-all'
                      }`}
                      title={importTitle || 'Import transactions to Opera'}
                    >
                      {isImporting ? <Loader2 className="h-5 w-5 animate-spin" /> : <CheckCircle className="h-5 w-5" />}
                      Import to Opera
                      {importReadiness && importReadiness.totalReady > 0 && (
                        <span className="bg-green-500 text-white text-sm px-2 py-0.5 rounded-full ml-1">
                          {importReadiness.totalReady}
                        </span>
                      )}
                    </button>

                    {/* Auto-allocate toggle */}
                    <label
                      className={`flex items-center gap-2 px-4 py-3 rounded-lg cursor-pointer transition-colors ${
                        autoAllocate
                          ? 'bg-purple-100 border-2 border-purple-400 text-purple-800'
                          : 'bg-white border-2 border-gray-300 text-gray-700 hover:bg-gray-50'
                      }`}
                      title="When enabled, receipts and payments are automatically matched to outstanding invoices after import"
                    >
                      <input
                        type="checkbox"
                        checked={autoAllocate}
                        onChange={(e) => setAutoAllocate(e.target.checked)}
                        className="rounded border-gray-300 text-purple-600 focus:ring-purple-500 h-4 w-4"
                      />
                      <span>Match payments to invoices</span>
                      {autoAllocate && <RefreshCw className="h-4 w-4 text-purple-600" />}
                    </label>
                  </div>

                  {dataSource === 'opera3' && (
                    <p className="mt-3 text-sm text-amber-600 flex items-center gap-1">
                      <AlertCircle className="h-4 w-4" />
                      Import not available for Opera 3 (read-only data source)
                    </p>
                  )}
                </div>

                {/* Errors */}
                {bankPreview.errors && bankPreview.errors.length > 0 && (
                  <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                    <h4 className="font-medium text-red-800 mb-2">Errors</h4>
                    <ul className="list-disc list-inside text-sm text-red-600">
                      {bankPreview.errors.map((err, idx) => (
                        <li key={idx}>{err}</li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            )}

            {/* Import Results */}
            {bankImportResult && (
              <div className={`p-4 rounded-lg ${bankImportResult.success ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'}`}>
                <div className="flex items-center gap-2 mb-2">
                  {bankImportResult.success ? (
                    <CheckCircle className="h-5 w-5 text-green-600" />
                  ) : (
                    <XCircle className="h-5 w-5 text-red-600" />
                  )}
                  <h3 className={`font-semibold ${bankImportResult.success ? 'text-green-800' : 'text-red-800'}`}>
                    {bankImportResult.success ? 'Import Completed' : 'Import Failed'}
                  </h3>
                </div>
                {bankImportResult.imported_transactions_count !== undefined && (
                  <div className="text-sm text-gray-700">
                    <p className="font-medium">
                      Imported {bankImportResult.imported_transactions_count} transactions
                      {bankImportResult.total_amount && ` totaling £${bankImportResult.total_amount.toFixed(2)}`}
                    </p>
                    {(bankImportResult.receipts_imported > 0 || bankImportResult.payments_imported > 0 || bankImportResult.refunds_imported > 0) && (
                      <div className="mt-1 text-xs space-x-3">
                        {bankImportResult.receipts_imported > 0 && (
                          <span className="text-green-600">{bankImportResult.receipts_imported} receipts</span>
                        )}
                        {bankImportResult.payments_imported > 0 && (
                          <span className="text-red-600">{bankImportResult.payments_imported} payments</span>
                        )}
                        {bankImportResult.refunds_imported > 0 && (
                          <span className="text-orange-600">{bankImportResult.refunds_imported} refunds</span>
                        )}
                        {bankImportResult.skipped_rejected > 0 && (
                          <span className="text-gray-500">{bankImportResult.skipped_rejected} rejected</span>
                        )}
                      </div>
                    )}
                  </div>
                )}
                {bankImportResult.error && (
                  <div className="mt-2">
                    <p className="text-sm font-medium text-red-700">{bankImportResult.error}</p>
                    {bankImportResult.message && (
                      <p className="text-sm text-red-600 mt-2 p-2 bg-red-100 rounded">{bankImportResult.message}</p>
                    )}
                  </div>
                )}
                {/* Show blocking repeat entries */}
                {bankImportResult.repeat_entries && bankImportResult.repeat_entries.length > 0 && (
                  <div className="mt-3 p-3 bg-orange-50 border border-orange-200 rounded">
                    <h4 className="font-medium text-orange-800 mb-2">Repeat Entries Requiring Action:</h4>
                    <ul className="text-sm text-orange-700 space-y-1">
                      {bankImportResult.repeat_entries.map((entry: any, idx: number) => (
                        <li key={idx} className="flex justify-between">
                          <span>{entry.entry_desc || entry.name}</span>
                          <span className="font-mono">{entry.amount < 0 ? '-' : ''}£{Math.abs(entry.amount).toFixed(2)}</span>
                        </li>
                      ))}
                    </ul>
                    <p className="text-xs text-orange-600 mt-2">
                      Go to Opera → Cashbook → Repeat Entries → Post routine, then re-preview this statement.
                    </p>
                  </div>
                )}
                {/* Show period violations */}
                {bankImportResult.period_violations && bankImportResult.period_violations.length > 0 && (
                  <div className="mt-3 p-3 bg-amber-50 border border-amber-200 rounded">
                    <h4 className="font-medium text-amber-800 mb-2">Period Violations - Cannot Import:</h4>
                    {bankImportResult.period_info && (
                      <p className="text-sm text-amber-600 mb-2">
                        Current period is {bankImportResult.period_info.current_period}/{bankImportResult.period_info.current_year}
                      </p>
                    )}
                    <ul className="text-sm text-amber-700 space-y-1">
                      {bankImportResult.period_violations.map((v: any, idx: number) => (
                        <li key={idx}>
                          <strong>{v.name || `Row ${v.row}`}</strong> ({v.date}) -
                          {v.ledger_name && <span className="text-amber-600"> {v.ledger_name}</span>}: {v.error}
                        </li>
                      ))}
                    </ul>
                    <p className="text-sm text-amber-600 mt-2">
                      Please adjust the dates or open the periods in Opera before importing.
                    </p>
                  </div>
                )}
                {bankImportResult.errors && bankImportResult.errors.length > 0 && (
                  <ul className="mt-2 list-disc list-inside text-sm text-red-600">
                    {bankImportResult.errors.map((err: any, idx: number) => (
                      <li key={idx}>Row {err.row}: {err.error}</li>
                    ))}
                  </ul>
                )}

                {/* Reconcile Section - shown after successful import */}
                {bankImportResult.success && showReconcilePrompt && (
                  <div className="mt-4 p-4 bg-green-50 border border-green-200 rounded-lg">
                    <div className="flex items-center justify-between mb-4">
                      <div className="flex items-center gap-2">
                        <Landmark className="h-5 w-5 text-green-600" />
                        <h4 className="font-semibold text-green-800">Step 4: Reconcile Statement</h4>
                        {loadingUnreconciled && (
                          <span className="text-sm text-gray-500 italic">Loading...</span>
                        )}
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-sm text-green-700">
                          {reconcileSelectedEntries.size} of {unreconciledEntries.length || 0} selected for reconciliation
                        </span>
                        <button
                          onClick={() => {
                            const allEntries = new Set(
                              unreconciledEntries
                                .filter((e: any) => e.ae_entry || e.entry_number)
                                .map((e: any) => e.ae_entry || e.entry_number)
                            );
                            setReconcileSelectedEntries(allEntries);
                          }}
                          className="px-2 py-1 text-xs bg-green-100 text-green-700 rounded hover:bg-green-200"
                        >
                          Select All
                        </button>
                        <button
                          onClick={() => setReconcileSelectedEntries(new Set())}
                          className="px-2 py-1 text-xs bg-gray-100 text-gray-600 rounded hover:bg-gray-200"
                        >
                          Clear
                        </button>
                      </div>
                    </div>

                    {/* Summary stats */}
                    <div className="mb-4 p-3 bg-white rounded border border-green-200 grid grid-cols-4 gap-4 text-sm">
                      <div>
                        <div className="text-gray-500">Statement Lines</div>
                        <div className="font-semibold text-lg">{bankPreview?.total_transactions || '-'}</div>
                      </div>
                      <div>
                        <div className="text-gray-500">Imported</div>
                        <div className="font-semibold text-lg text-green-600">{bankImportResult.imported_transactions_count || 0}</div>
                      </div>
                      <div>
                        <div className="text-gray-500">Ignored</div>
                        <div className="font-semibold text-lg text-gray-500">{ignoredTransactions.size}</div>
                      </div>
                      <div>
                        <div className="text-gray-500">Unreconciled in Opera</div>
                        <div className="font-semibold text-lg text-blue-600">{unreconciledEntries.length}</div>
                      </div>
                    </div>

                    {/* Side-by-side comparison: Statement vs Opera */}
                    <div className="bg-white rounded border border-green-200 overflow-hidden">
                      {/* Quick actions bar */}
                      <div className="flex items-center gap-2 px-3 py-2 bg-green-50 border-b border-green-200">
                        <button
                          onClick={() => {
                            // Select all imported transactions for reconciliation
                            const allImportedEntries = (bankImportResult.imported_transactions || [])
                              .map((t: any) => t.entry_number)
                              .filter(Boolean);
                            setReconcileSelectedEntries(new Set(allImportedEntries));
                          }}
                          className="px-3 py-1 text-xs bg-green-600 text-white rounded hover:bg-green-700"
                        >
                          Select All Imported ({(bankImportResult.imported_transactions || []).length})
                        </button>
                        <button
                          onClick={() => setReconcileSelectedEntries(new Set())}
                          className="px-3 py-1 text-xs bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
                        >
                          Clear Selection
                        </button>
                        <span className="text-xs text-gray-500 ml-auto">
                          Tip: References match automatically when imported - just select all and reconcile
                        </span>
                      </div>
                      <table className="w-full text-sm">
                        <thead className="bg-green-100">
                          <tr>
                            <th className="w-8 px-2 py-2 text-center border-r border-green-300">✓</th>
                            <th className="px-2 py-2 text-left border-r border-green-200 text-green-800" colSpan={4}>
                              Statement Transaction
                            </th>
                            <th className="px-2 py-2 text-center border-r border-green-200 text-green-800">Status</th>
                            <th className="px-2 py-2 text-left text-green-800" colSpan={3}>
                              Opera Entry
                            </th>
                          </tr>
                          <tr className="bg-green-50 text-xs text-gray-600">
                            <th className="border-r border-green-300"></th>
                            <th className="px-2 py-1 text-left border-r border-green-100">Date</th>
                            <th className="px-2 py-1 text-right border-r border-green-100">Amount</th>
                            <th className="px-2 py-1 text-left border-r border-green-100">Reference</th>
                            <th className="px-2 py-1 text-left border-r border-green-200">Description</th>
                            <th className="px-2 py-1 text-center border-r border-green-200"></th>
                            <th className="px-2 py-1 text-left border-r border-green-100">Entry #</th>
                            <th className="px-2 py-1 text-left border-r border-green-100">Reference</th>
                            <th className="px-2 py-1 text-left">Comment</th>
                          </tr>
                        </thead>
                        <tbody>
                          {/* Show all statement transactions with their status */}
                          {(() => {
                            // Combine all transaction types from bankPreview
                            const allStmtTxns = [
                              ...(bankPreview?.matched_receipts || []),
                              ...(bankPreview?.matched_payments || []),
                              ...(bankPreview?.matched_refunds || []),
                              ...(bankPreview?.unmatched || []),
                              ...(bankPreview?.skipped || []),
                            ].sort((a, b) => a.row - b.row);

                            // Map imported transactions by row for quick lookup
                            const importedByRow = new Map<number, any>(
                              (bankImportResult.imported_transactions || []).map((t: any) => [t.row as number, t])
                            );

                            return allStmtTxns.map((txn: any, idx: number) => {
                              const isIgnored = ignoredTransactions.has(txn.row);
                              const importedTxn = importedByRow.get(txn.row) as any;
                              const isImported = !!importedTxn;
                              const entryNumber = importedTxn?.entry_number as string | undefined;

                              // Find matching unreconciled Opera entry
                              const matchedOperaEntry = entryNumber
                                ? unreconciledEntries.find((e: any) => (e.ae_entry || e.entry_number) === entryNumber)
                                : null;

                              let statusBadge;
                              let rowClass = 'hover:bg-gray-50';
                              if (isIgnored) {
                                statusBadge = <span className="px-2 py-0.5 text-xs bg-gray-200 text-gray-600 rounded">Ignored</span>;
                                rowClass = 'bg-gray-50';
                              } else if (isImported) {
                                statusBadge = <span className="px-2 py-0.5 text-xs bg-green-100 text-green-700 rounded">Imported</span>;
                                rowClass = entryNumber && reconcileSelectedEntries.has(entryNumber) ? 'bg-green-100' : 'bg-green-50';
                              } else if (txn.is_duplicate) {
                                statusBadge = <span className="px-2 py-0.5 text-xs bg-amber-100 text-amber-700 rounded">Duplicate</span>;
                                rowClass = 'bg-amber-50';
                              } else {
                                statusBadge = <span className="px-2 py-0.5 text-xs bg-blue-100 text-blue-600 rounded">Pending</span>;
                              }

                              return (
                                <tr key={idx} className={`border-t border-green-100 ${rowClass}`}>
                                  <td className="px-2 py-2 text-center border-r border-green-300">
                                    {isImported && entryNumber && (
                                      <input
                                        type="checkbox"
                                        checked={reconcileSelectedEntries.has(entryNumber)}
                                        onChange={(e) => {
                                          const newSet = new Set(reconcileSelectedEntries);
                                          if (e.target.checked) {
                                            newSet.add(entryNumber);
                                          } else {
                                            newSet.delete(entryNumber);
                                          }
                                          setReconcileSelectedEntries(newSet);
                                        }}
                                        className="w-4 h-4 text-green-600 rounded"
                                      />
                                    )}
                                  </td>
                                  {/* Statement side */}
                                  <td className="px-2 py-2 border-r border-green-100 text-gray-600 whitespace-nowrap">
                                    {txn.date?.split('T')[0] || txn.date || '-'}
                                  </td>
                                  <td className={`px-2 py-2 border-r border-green-100 text-right font-mono whitespace-nowrap ${
                                    txn.amount < 0 ? 'text-red-600' : 'text-green-600'
                                  }`}>
                                    {txn.amount < 0 ? '-' : '+'}£{Math.abs(txn.amount || 0).toFixed(2)}
                                  </td>
                                  <td className="px-2 py-2 border-r border-green-100 text-gray-600 font-mono text-xs max-w-[100px] truncate" title={txn.reference || ''}>
                                    {txn.reference || <span className="text-gray-300">-</span>}
                                  </td>
                                  <td className="px-2 py-2 border-r border-green-200 text-gray-700 max-w-[150px] truncate" title={txn.name || txn.memo}>
                                    {(txn.name || txn.memo || '-').substring(0, 25)}
                                  </td>
                                  {/* Status */}
                                  <td className="px-2 py-2 text-center border-r border-green-200">
                                    {statusBadge}
                                  </td>
                                  {/* Opera side */}
                                  <td className="px-2 py-2 border-r border-green-100 font-mono text-blue-600 whitespace-nowrap">
                                    {entryNumber || <span className="text-gray-300">-</span>}
                                  </td>
                                  <td className="px-2 py-2 border-r border-green-100 text-gray-600 font-mono text-xs max-w-[100px] truncate" title={(importedTxn as any)?.reference || ''}>
                                    {(importedTxn as any)?.reference || <span className="text-gray-300">-</span>}
                                  </td>
                                  <td className="px-2 py-2 text-gray-600 text-xs max-w-[150px] truncate" title={(importedTxn as any)?.name || (importedTxn as any)?.memo || ''}>
                                    {(importedTxn as any)?.name || (importedTxn as any)?.memo || <span className="text-gray-300">-</span>}
                                  </td>
                                </tr>
                              );
                            });
                          })()}
                        </tbody>
                      </table>
                    </div>

                    {/* Reconcile action buttons */}
                    <div className="mt-4 flex items-center justify-between">
                      <div className="text-sm text-green-700">
                        <strong>{reconcileSelectedEntries.size}</strong> entries ready to reconcile
                        {reconcileSelectedEntries.size > 0 && (
                          <span className="ml-2 text-gray-500">
                            (Total: £{(bankImportResult.imported_transactions || [])
                              .filter((t: any) => reconcileSelectedEntries.has(t.entry_number))
                              .reduce((sum: number, t: any) => sum + Math.abs(t.amount || 0), 0)
                              .toFixed(2)})
                          </span>
                        )}
                      </div>
                      <div className="flex gap-2">
                        <button
                          onClick={async () => {
                            if (reconcileSelectedEntries.size === 0) {
                              alert('Please select entries to reconcile');
                              return;
                            }
                            try {
                              const entries = Array.from(reconcileSelectedEntries).map((entryNum, idx) => ({
                                entry_number: entryNum,
                                statement_line: (idx + 1) * 10
                              }));

                              // Get the latest date from selected imported transactions
                              const selectedEntryDates = (bankImportResult.imported_transactions || [])
                                .filter((t: any) => reconcileSelectedEntries.has(t.entry_number))
                                .map((t: any) => {
                                  const d = t.date;
                                  return typeof d === 'string' ? d.split('T')[0] : d;
                                })
                                .filter(Boolean)
                                .sort();
                              const latestDate = selectedEntryDates.pop() || new Date().toISOString().split('T')[0];

                              const response = await authFetch(
                                `/api/reconcile/bank/${selectedBankCode}/mark-reconciled`,
                                {
                                  method: 'POST',
                                  headers: { 'Content-Type': 'application/json' },
                                  body: JSON.stringify({
                                    entries,
                                    statement_date: latestDate,
                                    reconciliation_date: latestDate
                                  })
                                }
                              );
                              const data = await response.json();
                              if (data.success) {
                                alert(`✓ Successfully reconciled ${data.records_reconciled} entries!\n\nReconciliation complete in Opera.`);
                                // Refresh unreconciled entries
                                const res = await authFetch(`${API_BASE}/bank-reconciliation/unreconciled-entries?bank_code=${selectedBankCode}`);
                                const refreshData = await res.json();
                                if (refreshData.success && refreshData.entries) {
                                  setUnreconciledEntries(refreshData.entries);
                                }
                                setReconcileSelectedEntries(new Set());
                                // If all entries reconciled, hide the prompt
                                if (refreshData.entries?.length === 0) {
                                  setShowReconcilePrompt(false);
                                }
                              } else {
                                alert(`Reconciliation failed: ${data.error || data.errors?.join(', ')}`);
                              }
                            } catch (error) {
                              alert(`Failed to reconcile: ${error}`);
                            }
                          }}
                          disabled={reconcileSelectedEntries.size === 0}
                          className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium flex items-center gap-2"
                        >
                          <CheckCircle className="h-4 w-4" />
                          Reconcile Selected ({reconcileSelectedEntries.size})
                        </button>
                        <button
                          onClick={() => {
                            setShowReconcilePrompt(false);
                            setUnreconciledEntries([]);
                            setReconcileSelectedEntries(new Set());
                          }}
                          className="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300 text-sm font-medium"
                        >
                          Skip for Now
                        </button>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>

          {/* Help */}
          <div className="mt-6 bg-blue-50 rounded-lg p-4 border border-blue-200">
            <div className="flex items-start gap-3">
              <AlertCircle className="h-5 w-5 text-blue-600 flex-shrink-0 mt-0.5" />
              <div>
                <h3 className="font-semibold text-blue-800">Bank Statement Import</h3>
                <div className="text-sm text-blue-700 mt-1 space-y-2">
                  <p>Import transactions from bank statement files (CSV, OFX, QIF, MT940).</p>
                  <p>The system will automatically match transactions to customers/suppliers using fuzzy name matching.</p>
                  <div className="bg-white/50 rounded p-2 mt-2">
                    <p className="font-medium text-blue-800 mb-1">Workflow:</p>
                    <ol className="list-decimal list-inside space-y-1 text-blue-700">
                      <li>Click "Analyse Transactions" to analyze the bank statement</li>
                      <li>Review matched receipts (green) and payments (red)</li>
                      <li>For unmatched transactions (amber), select an account from the dropdown</li>
                      <li>Use checkboxes to bulk-assign multiple transactions at once</li>
                      <li>Click "Import Transactions" when ready</li>
                    </ol>
                  </div>
                  {dataSource === 'opera-sql' ? (
                    <p className="font-medium mt-2">Opera SQL SE: Full import functionality available.</p>
                  ) : (
                    <p className="font-medium text-amber-700 mt-2">Opera 3: Preview only (read-only data source).</p>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Form (for other import types) */}
      {activeType !== 'bank-statement' && (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4">
          {importTypes.find(t => t.id === activeType)?.label}
        </h2>

        <div className="space-y-6">
          {/* Common Fields Row */}
          <div className="grid grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Post Date</label>
              <input
                type="date"
                value={postDate}
                onChange={e => setPostDate(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
            {(activeType === 'sales-receipt' || activeType === 'purchase-payment') && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Bank Account</label>
                <input
                  type="text"
                  value={bankAccount}
                  onChange={e => setBankAccount(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="BC010"
                />
              </div>
            )}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Reference</label>
              <input
                type="text"
                value={reference}
                onChange={e => setReference(e.target.value)}
                maxLength={20}
                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                placeholder="e.g., INV12345"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Input By</label>
              <input
                type="text"
                value={inputBy}
                onChange={e => setInputBy(e.target.value)}
                maxLength={8}
                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
              />
            </div>
          </div>

          {/* Sales Receipt Fields */}
          {activeType === 'sales-receipt' && (
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Customer Account *</label>
                <input
                  type="text"
                  value={customerAccount}
                  onChange={e => setCustomerAccount(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="e.g., A046"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Amount (GBP) *</label>
                <input
                  type="number"
                  value={receiptAmount}
                  onChange={e => setReceiptAmount(e.target.value)}
                  step="0.01"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="100.00"
                />
              </div>
            </div>
          )}

          {/* Purchase Payment Fields */}
          {activeType === 'purchase-payment' && (
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Supplier Account *</label>
                <input
                  type="text"
                  value={supplierAccount}
                  onChange={e => setSupplierAccount(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="e.g., P001"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Amount (GBP) *</label>
                <input
                  type="number"
                  value={paymentAmount}
                  onChange={e => setPaymentAmount(e.target.value)}
                  step="0.01"
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="500.00"
                />
              </div>
            </div>
          )}

          {/* Sales Invoice Fields */}
          {activeType === 'sales-invoice' && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Customer Account *</label>
                  <input
                    type="text"
                    value={customerAccount}
                    onChange={e => setCustomerAccount(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="e.g., A046"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Invoice Number *</label>
                  <input
                    type="text"
                    value={invoiceNumber}
                    onChange={e => setInvoiceNumber(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="e.g., INV001"
                  />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Net Amount (GBP) *</label>
                  <input
                    type="number"
                    value={netAmount}
                    onChange={e => setNetAmount(e.target.value)}
                    step="0.01"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="1000.00"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">VAT Amount (GBP)</label>
                  <input
                    type="number"
                    value={vatAmount}
                    onChange={e => setVatAmount(e.target.value)}
                    step="0.01"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="200.00"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Sales Nominal</label>
                  <input
                    type="text"
                    value={nominalAccount}
                    onChange={e => setNominalAccount(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="GA010"
                  />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
                <input
                  type="text"
                  value={description}
                  onChange={e => setDescription(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="Invoice description"
                />
              </div>
            </div>
          )}

          {/* Purchase Invoice Fields */}
          {activeType === 'purchase-invoice' && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Supplier Account *</label>
                  <input
                    type="text"
                    value={supplierAccount}
                    onChange={e => setSupplierAccount(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="e.g., P001"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Invoice Number *</label>
                  <input
                    type="text"
                    value={invoiceNumber}
                    onChange={e => setInvoiceNumber(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="e.g., PINV001"
                  />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Net Amount (GBP) *</label>
                  <input
                    type="number"
                    value={netAmount}
                    onChange={e => setNetAmount(e.target.value)}
                    step="0.01"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="500.00"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">VAT Amount (GBP)</label>
                  <input
                    type="number"
                    value={vatAmount}
                    onChange={e => setVatAmount(e.target.value)}
                    step="0.01"
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="100.00"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Expense Nominal</label>
                  <input
                    type="text"
                    value={nominalAccount}
                    onChange={e => setNominalAccount(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                    placeholder="HA010"
                  />
                </div>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
                <input
                  type="text"
                  value={description}
                  onChange={e => setDescription(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="Invoice description"
                />
              </div>
            </div>
          )}

          {/* Nominal Journal Fields */}
          {activeType === 'nominal-journal' && (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Description</label>
                <input
                  type="text"
                  value={description}
                  onChange={e => setDescription(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  placeholder="Journal description"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Journal Lines</label>
                <div className="space-y-2">
                  {journalLines.map((line, idx) => (
                    <div key={idx} className="flex gap-2 items-center">
                      <input
                        type="text"
                        value={line.account}
                        onChange={e => updateJournalLine(idx, 'account', e.target.value)}
                        className="w-32 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                        placeholder="Account"
                      />
                      <input
                        type="number"
                        value={line.amount}
                        onChange={e => updateJournalLine(idx, 'amount', e.target.value)}
                        step="0.01"
                        className="w-32 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                        placeholder="Amount"
                      />
                      <input
                        type="text"
                        value={line.description}
                        onChange={e => updateJournalLine(idx, 'description', e.target.value)}
                        className="flex-1 px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                        placeholder="Description"
                      />
                      {journalLines.length > 2 && (
                        <button
                          onClick={() => removeJournalLine(idx)}
                          className="text-red-500 hover:text-red-700"
                        >
                          &times;
                        </button>
                      )}
                    </div>
                  ))}
                </div>
                <div className="flex justify-between items-center mt-2">
                  <button
                    onClick={addJournalLine}
                    className="text-blue-600 hover:text-blue-800 text-sm font-medium"
                  >
                    + Add Line
                  </button>
                  <div className={`text-sm font-medium ${Math.abs(journalTotal) < 0.01 ? 'text-green-600' : 'text-red-600'}`}>
                    Total: {journalTotal >= 0 ? '' : '-'}£{Math.abs(journalTotal).toFixed(2)}
                    {Math.abs(journalTotal) < 0.01 ? ' (Balanced)' : ' (Must be £0.00)'}
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Validate Only Checkbox and Submit */}
          <div className="flex items-center justify-between pt-4 border-t">
            <label className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={validateOnly}
                onChange={e => setValidateOnly(e.target.checked)}
                className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
              />
              <span className="text-sm text-gray-700">Validate only (don't import)</span>
            </label>

            <button
              onClick={handleImport}
              disabled={loading || !bankPreview}
              className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center gap-2"
              title={!bankPreview ? 'Run Analyse Transactions first' : ''}
            >
              {loading ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Processing...
                </>
              ) : validateOnly ? (
                'Validate'
              ) : (
                'Import'
              )}
            </button>
          </div>
        </div>
      </div>
      )}

      {/* Results (for non-bank-statement imports) */}
      {activeType !== 'bank-statement' && result && (
        <div className={`rounded-lg shadow p-6 ${
          result.success ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'
        }`}>
          <div className="flex items-start gap-3">
            {result.success ? (
              <CheckCircle className="h-6 w-6 text-green-600 flex-shrink-0" />
            ) : (
              <XCircle className="h-6 w-6 text-red-600 flex-shrink-0" />
            )}
            <div className="flex-1">
              <h3 className={`font-semibold ${result.success ? 'text-green-800' : 'text-red-800'}`}>
                {result.success
                  ? (result.validate_only ? 'Validation Successful' : 'Import Successful')
                  : 'Import Failed'
                }
              </h3>

              {result.details && result.details.length > 0 && (
                <div className="mt-2">
                  <ul className="list-disc list-inside text-sm text-gray-600 space-y-1">
                    {result.details.map((detail, i) => (
                      <li key={i}>{detail}</li>
                    ))}
                  </ul>
                </div>
              )}

              {result.errors && result.errors.length > 0 && (
                <div className="mt-2">
                  <ul className="list-disc list-inside text-sm text-red-600 space-y-1">
                    {result.errors.map((error, i) => (
                      <li key={i}>{error}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Help Section (for non-bank-statement imports) */}
      {activeType !== 'bank-statement' && (
      <div className="bg-blue-50 rounded-lg p-6 border border-blue-200">
        <div className="flex items-start gap-3">
          <AlertCircle className="h-5 w-5 text-blue-600 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="font-semibold text-blue-800">
              {activeType === 'sales-receipt' && 'Sales Receipt Help'}
              {activeType === 'purchase-payment' && 'Purchase Payment Help'}
              {activeType === 'sales-invoice' && 'Sales Invoice Help'}
              {activeType === 'purchase-invoice' && 'Purchase Invoice Help'}
              {activeType === 'nominal-journal' && 'Nominal Journal Help'}
            </h3>
            <div className="text-sm text-blue-700 mt-1 space-y-1">
              {activeType === 'sales-receipt' && (
                <>
                  <p>Records a payment received from a customer.</p>
                  <p>Creates: aentry, atran, and ntran (Debit Bank, Credit SL Control)</p>
                </>
              )}
              {activeType === 'purchase-payment' && (
                <>
                  <p>Records a payment made to a supplier.</p>
                  <p>Creates: aentry, atran, and ntran (Credit Bank, Debit PL Control)</p>
                </>
              )}
              {activeType === 'sales-invoice' && (
                <>
                  <p>Posts a sales invoice to the nominal ledger.</p>
                  <p>Creates: ntran (Debit SL Control, Credit Sales, Credit VAT)</p>
                </>
              )}
              {activeType === 'purchase-invoice' && (
                <>
                  <p>Posts a purchase invoice to the nominal ledger.</p>
                  <p>Creates: ntran (Credit PL Control, Debit Expense, Debit VAT)</p>
                </>
              )}
              {activeType === 'nominal-journal' && (
                <>
                  <p>Posts a manual journal entry. Journal must balance (total = £0.00).</p>
                  <p>Positive amounts = Debit, Negative amounts = Credit</p>
                </>
              )}
            </div>
          </div>
        </div>
      </div>
      )}

      {/* Import History Modal */}
      {showImportHistory && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-full max-w-4xl max-h-[85vh] overflow-hidden">
            <div className="flex items-center justify-between p-4 border-b">
              <h2 className="text-lg font-semibold flex items-center gap-2">
                <History className="h-5 w-5 text-blue-600" />
                Bank Statement Import History
              </h2>
              <button onClick={() => setShowImportHistory(false)} className="text-gray-400 hover:text-gray-600">
                <X className="h-5 w-5" />
              </button>
            </div>

            {/* Filters */}
            <div className="p-4 border-b bg-gray-50 flex flex-wrap items-end gap-4">
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">From Date</label>
                <input
                  type="date"
                  value={historyFromDate}
                  onChange={(e) => setHistoryFromDate(e.target.value)}
                  className="text-sm border border-gray-300 rounded px-2 py-1"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">To Date</label>
                <input
                  type="date"
                  value={historyToDate}
                  onChange={(e) => setHistoryToDate(e.target.value)}
                  className="text-sm border border-gray-300 rounded px-2 py-1"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-600 mb-1">Show</label>
                <select
                  value={historyLimit}
                  onChange={(e) => setHistoryLimit(Number(e.target.value))}
                  className="text-sm border border-gray-300 rounded px-2 py-1"
                >
                  <option value={10}>Last 10</option>
                  <option value={25}>Last 25</option>
                  <option value={50}>Last 50</option>
                  <option value={100}>Last 100</option>
                </select>
              </div>
              <button
                onClick={() => fetchImportHistory(historyLimit, historyFromDate, historyToDate)}
                className="px-3 py-1 bg-blue-600 text-white text-sm rounded hover:bg-blue-700"
              >
                Filter
              </button>
              <button
                onClick={() => { setHistoryFromDate(''); setHistoryToDate(''); fetchImportHistory(historyLimit); }}
                className="px-3 py-1 bg-gray-200 text-gray-700 text-sm rounded hover:bg-gray-300"
              >
                Reset
              </button>
              <div className="flex-1" />
              <button
                onClick={() => setShowClearConfirm(true)}
                disabled={isClearing}
                className="px-3 py-1 bg-red-600 text-white text-sm rounded hover:bg-red-700 disabled:bg-gray-400"
              >
                {isClearing ? 'Clearing...' : 'Clear History'}
              </button>
            </div>

            <div className="overflow-y-auto max-h-[55vh] p-4">
              {importHistoryLoading ? (
                <div className="text-center py-8 text-gray-500">Loading...</div>
              ) : importHistoryData.length === 0 ? (
                <div className="text-center py-8 text-gray-500">No import history found</div>
              ) : (
                <table className="w-full text-sm">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="w-8 p-2"></th>
                      <th className="text-left p-2 font-medium text-gray-600">Date</th>
                      <th className="text-left p-2 font-medium text-gray-600">Filename</th>
                      <th className="text-center p-2 font-medium text-gray-600">Source</th>
                      <th className="text-center p-2 font-medium text-gray-600">Bank</th>
                      <th className="text-right p-2 font-medium text-gray-600">Receipts</th>
                      <th className="text-right p-2 font-medium text-gray-600">Payments</th>
                      <th className="text-center p-2 font-medium text-gray-600">Txns</th>
                      <th className="text-center p-2 font-medium text-gray-600">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {importHistoryData.map((h) => (
                      <React.Fragment key={h.id}>
                        <tr className={`hover:bg-gray-50 cursor-pointer ${expandedHistoryId === h.id ? 'bg-blue-50' : ''}`}>
                          <td className="p-2 text-center">
                            <button
                              onClick={() => setExpandedHistoryId(expandedHistoryId === h.id ? null : h.id)}
                              className="p-1 hover:bg-gray-200 rounded"
                            >
                              {expandedHistoryId === h.id ? (
                                <ChevronDown className="h-4 w-4 text-gray-500" />
                              ) : (
                                <ChevronRight className="h-4 w-4 text-gray-500" />
                              )}
                            </button>
                          </td>
                          <td className="p-2 text-gray-900" onClick={() => setExpandedHistoryId(expandedHistoryId === h.id ? null : h.id)}>
                            {new Date(h.import_date).toLocaleDateString()}
                          </td>
                          <td className="p-2 text-gray-600 text-xs" onClick={() => setExpandedHistoryId(expandedHistoryId === h.id ? null : h.id)}>
                            <span className="font-mono">{h.filename || '-'}</span>
                          </td>
                          <td className="p-2 text-center" onClick={() => setExpandedHistoryId(expandedHistoryId === h.id ? null : h.id)}>
                            <span className={`px-2 py-0.5 rounded text-xs ${h.source === 'file' ? 'bg-green-100 text-green-700' : 'bg-purple-100 text-purple-700'}`}>
                              {h.source === 'file' ? 'File' : 'Email'}
                            </span>
                          </td>
                          <td className="p-2 text-center text-gray-600 font-mono text-xs" onClick={() => setExpandedHistoryId(expandedHistoryId === h.id ? null : h.id)}>
                            {h.bank_code || '-'}
                          </td>
                          <td className="p-2 text-right text-green-600" onClick={() => setExpandedHistoryId(expandedHistoryId === h.id ? null : h.id)}>
                            £{(h.total_receipts || 0).toFixed(2)}
                          </td>
                          <td className="p-2 text-right text-red-600" onClick={() => setExpandedHistoryId(expandedHistoryId === h.id ? null : h.id)}>
                            £{(h.total_payments || 0).toFixed(2)}
                          </td>
                          <td className="p-2 text-center text-gray-600" onClick={() => setExpandedHistoryId(expandedHistoryId === h.id ? null : h.id)}>
                            {h.transactions_imported || 0}
                          </td>
                          <td className="p-2 text-center">
                            <button
                              onClick={() => setReImportRecord({ id: h.id, filename: h.filename || 'Unknown', amount: (h.total_receipts || 0) + (h.total_payments || 0) })}
                              className="px-2 py-1 text-xs bg-amber-100 text-amber-700 rounded hover:bg-amber-200"
                              title="Remove from history to allow re-importing"
                            >
                              Re-import
                            </button>
                          </td>
                        </tr>
                        {/* Expanded Detail Row */}
                        {expandedHistoryId === h.id && (
                          <tr key={`${h.id}-detail`} className="bg-gray-50">
                            <td colSpan={9} className="p-4">
                              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                                <div>
                                  <div className="text-gray-500 text-xs uppercase tracking-wide mb-1">Import Date & Time</div>
                                  <div className="font-medium">
                                    {new Date(h.import_date).toLocaleDateString()} at {new Date(h.import_date).toLocaleTimeString()}
                                  </div>
                                </div>
                                <div>
                                  <div className="text-gray-500 text-xs uppercase tracking-wide mb-1">Target System</div>
                                  <div className="font-medium">
                                    <span className={`px-2 py-0.5 rounded text-xs ${h.target_system === 'opera3' ? 'bg-orange-100 text-orange-700' : 'bg-blue-100 text-blue-700'}`}>
                                      {h.target_system === 'opera3' ? 'Opera 3' : 'Opera SQL SE'}
                                    </span>
                                  </div>
                                </div>
                                <div>
                                  <div className="text-gray-500 text-xs uppercase tracking-wide mb-1">Imported By</div>
                                  <div className="font-medium font-mono text-xs">{h.imported_by || '-'}</div>
                                </div>
                                <div>
                                  <div className="text-gray-500 text-xs uppercase tracking-wide mb-1">Record ID</div>
                                  <div className="font-medium font-mono text-xs">#{h.id}</div>
                                </div>

                                {/* Email Details (if from email) */}
                                {h.source === 'email' && (
                                  <>
                                    <div className="col-span-2">
                                      <div className="text-gray-500 text-xs uppercase tracking-wide mb-1">Email Subject</div>
                                      <div className="font-medium text-xs">{h.email_subject || '-'}</div>
                                    </div>
                                    <div className="col-span-2">
                                      <div className="text-gray-500 text-xs uppercase tracking-wide mb-1">From</div>
                                      <div className="font-medium text-xs">{h.email_from || '-'}</div>
                                    </div>
                                  </>
                                )}

                                {/* Summary */}
                                <div className="col-span-2 md:col-span-4 mt-2 pt-3 border-t border-gray-200">
                                  <div className="flex items-center gap-6">
                                    <div className="flex items-center gap-2">
                                      <span className="text-gray-500 text-xs">Receipts:</span>
                                      <span className="font-semibold text-green-600">£{(h.total_receipts || 0).toFixed(2)}</span>
                                    </div>
                                    <div className="flex items-center gap-2">
                                      <span className="text-gray-500 text-xs">Payments:</span>
                                      <span className="font-semibold text-red-600">£{(h.total_payments || 0).toFixed(2)}</span>
                                    </div>
                                    <div className="flex items-center gap-2">
                                      <span className="text-gray-500 text-xs">Net:</span>
                                      <span className={`font-semibold ${((h.total_receipts || 0) - (h.total_payments || 0)) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                        £{((h.total_receipts || 0) - (h.total_payments || 0)).toFixed(2)}
                                      </span>
                                    </div>
                                    <div className="flex items-center gap-2">
                                      <span className="text-gray-500 text-xs">Transactions:</span>
                                      <span className="font-semibold">{h.transactions_imported || 0}</span>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            </td>
                          </tr>
                        )}
                      </React.Fragment>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Clear History Confirmation */}
      {showClearConfirm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 max-w-md">
            <h3 className="text-lg font-semibold text-gray-900 mb-2">Clear Import History?</h3>
            <p className="text-gray-600 mb-4">
              This will permanently delete import history records
              {historyFromDate || historyToDate ? ' within the selected date range' : ''}.
              This action cannot be undone.
            </p>
            <div className="flex justify-end gap-3">
              <button
                onClick={() => setShowClearConfirm(false)}
                className="px-4 py-2 text-gray-700 border border-gray-300 rounded hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={clearImportHistory}
                className="px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
              >
                Clear History
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Clear Statement Confirmation */}
      {showClearStatementConfirm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 max-w-md">
            <h3 className="text-lg font-semibold text-gray-900 mb-2">Clear Statement?</h3>
            <p className="text-gray-600 mb-2">
              Are you sure you want to clear the current statement?
            </p>
            <div className="bg-amber-50 border border-amber-200 rounded p-3 mb-4 text-sm text-amber-800">
              <strong>Warning:</strong> The following will be lost:
              <ul className="list-disc list-inside mt-1 text-amber-700">
                <li>All account assignments you've made</li>
                <li>Transaction type selections</li>
                <li>Date overrides</li>
                <li>Selected/deselected items</li>
              </ul>
              <p className="mt-2 text-xs">
                Tip: Any transactions you excluded can still be entered manually in Opera.
              </p>
            </div>
            <div className="flex justify-end gap-3">
              <button
                onClick={() => setShowClearStatementConfirm(false)}
                className="px-4 py-2 text-gray-700 border border-gray-300 rounded hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={() => {
                  setBankPreview(null);
                  setBankImportResult(null);
                  setEditedTransactions(new Map());
                  setIncludedSkipped(new Map());
                  setTransactionTypeOverrides(new Map());
                  setRefundOverrides(new Map());
                  setSelectedForImport(new Set());
                  setDateOverrides(new Map());
                  setAutoAllocateDisabled(new Set());
                  clearPersistedState();
                  setShowClearStatementConfirm(false);
                }}
                className="px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
              >
                Clear Statement
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Ignore Transaction Confirmation */}
      {ignoreConfirm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 max-w-md">
            <h3 className="text-lg font-semibold text-gray-900 mb-2">Ignore Transaction?</h3>
            <p className="text-gray-600 mb-2">
              Are you sure you want to ignore this transaction? It won't appear in future reconciliations.
            </p>
            <div className="bg-gray-50 p-3 rounded mb-4">
              <div className="text-sm text-gray-500">Date: {ignoreConfirm.date}</div>
              <div className="font-mono text-sm">{ignoreConfirm.description}</div>
              <div className={`text-sm font-medium ${ignoreConfirm.amount >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                £{Math.abs(ignoreConfirm.amount).toFixed(2)} {ignoreConfirm.amount >= 0 ? '(Receipt)' : '(Payment)'}
              </div>
            </div>
            <p className="text-amber-600 text-sm mb-4">
              <strong>Note:</strong> Use this for transactions already entered manually in Opera.
              You can view/manage ignored transactions in Bank Reconciliation.
            </p>
            <div className="flex justify-end gap-3">
              <button
                onClick={() => setIgnoreConfirm(null)}
                className="px-4 py-2 text-gray-700 border border-gray-300 rounded hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={handleIgnoreTransaction}
                disabled={isIgnoring}
                className="px-4 py-2 bg-amber-600 text-white rounded hover:bg-amber-700 disabled:bg-gray-400"
              >
                {isIgnoring ? 'Ignoring...' : 'Yes, Ignore'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Re-import Confirmation */}
      {reImportRecord && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl p-6 max-w-md">
            <h3 className="text-lg font-semibold text-gray-900 mb-2">Allow Re-import?</h3>
            <p className="text-gray-600 mb-2">
              This will remove the import record for:
            </p>
            <div className="bg-gray-50 p-3 rounded mb-4">
              <div className="font-mono text-sm">{reImportRecord.filename}</div>
              <div className="text-sm text-gray-500">Total: £{reImportRecord.amount.toFixed(2)}</div>
            </div>
            <p className="text-amber-600 text-sm mb-4">
              <strong>Note:</strong> This does NOT remove transactions from Opera.
              Only use this if you have restored Opera data and need to re-import.
            </p>
            <div className="flex justify-end gap-3">
              <button
                onClick={() => setReImportRecord(null)}
                className="px-4 py-2 text-gray-700 border border-gray-300 rounded hover:bg-gray-50"
              >
                Cancel
              </button>
              <button
                onClick={deleteHistoryRecord}
                disabled={isDeleting}
                className="px-4 py-2 bg-amber-600 text-white rounded hover:bg-amber-700 disabled:bg-gray-400"
              >
                {isDeleting ? 'Removing...' : 'Remove from History'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
