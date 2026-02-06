import { useState, useEffect, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import { FileText, CheckCircle, XCircle, AlertCircle, Loader2, Receipt, CreditCard, FileSpreadsheet, BookOpen, Landmark, Upload, Edit3, RefreshCw, Search, RotateCcw } from 'lucide-react';

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

type TransactionType = 'sales_receipt' | 'purchase_payment' | 'sales_refund' | 'purchase_refund';

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
  // For editable preview
  manual_account?: string;
  manual_ledger_type?: 'C' | 'S';
  isEdited?: boolean;
}

interface EnhancedBankImportPreview {
  success: boolean;
  filename: string;
  detected_format?: string;
  total_transactions: number;
  matched_receipts: BankImportTransaction[];
  matched_payments: BankImportTransaction[];
  matched_refunds: BankImportTransaction[];
  unmatched: BankImportTransaction[];
  already_posted: BankImportTransaction[];
  skipped: BankImportTransaction[];
  summary?: {
    to_import: number;
    refund_count: number;
    unmatched_count: number;
    already_posted_count: number;
    skipped_count: number;
  };
  errors: string[];
}

type PreviewTab = 'receipts' | 'payments' | 'refunds' | 'unmatched' | 'skipped';

const API_BASE = 'http://localhost:8000/api';

type ImportType = 'bank-statement' | 'sales-receipt' | 'purchase-payment' | 'sales-invoice' | 'purchase-invoice' | 'nominal-journal';

type DataSource = 'opera-sql' | 'opera3';

export function Imports({ bankRecOnly = false }: { bankRecOnly?: boolean } = {}) {
  const [activeType, setActiveType] = useState<ImportType>('bank-statement');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<ImportResult | null>(null);
  const [validateOnly, setValidateOnly] = useState(true);

  // Data source derived from Opera settings configuration
  const { data: operaConfigData } = useQuery({
    queryKey: ['operaConfig'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/config/opera`);
      return res.json();
    },
  });
  const dataSource: DataSource = operaConfigData?.version === 'opera3' ? 'opera3' : 'opera-sql';

  // Bank statement import state
  const [bankAccounts, setBankAccounts] = useState<BankAccount[]>([]);
  const [selectedBankCode, setSelectedBankCode] = useState(() =>
    localStorage.getItem('bankImport_bankCode') || 'BC010'
  );
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
  const [bankPreview, setBankPreview] = useState<EnhancedBankImportPreview | null>(null);
  const [bankImportResult, setBankImportResult] = useState<any>(null);

  // New state for editable preview
  const [editedTransactions, setEditedTransactions] = useState<Map<number, BankImportTransaction>>(new Map());
  const [selectedRows, setSelectedRows] = useState<Set<number>>(new Set());

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

  // Selection state for import - tracks which rows are selected for import across ALL tabs
  const [selectedForImport, setSelectedForImport] = useState<Set<number>>(new Set());

  // Fetch customers and suppliers using react-query (auto-refreshes on company switch)
  const { data: customersData } = useQuery({
    queryKey: ['bank-import-customers'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/bank-import/accounts/customers`);
      return res.json();
    },
  });

  const { data: suppliersData } = useQuery({
    queryKey: ['bank-import-suppliers'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/bank-import/accounts/suppliers`);
      return res.json();
    },
  });

  const customers: OperaAccount[] = customersData?.success ? customersData.accounts : [];
  const suppliers: OperaAccount[] = suppliersData?.success ? suppliersData.accounts : [];

  // Fetch CSV files in the selected directory
  const { data: csvFilesData } = useQuery({
    queryKey: ['csv-files', csvDirectory],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/bank-import/list-csv?directory=${encodeURIComponent(csvDirectory)}`);
      return res.json();
    },
    enabled: !!csvDirectory,
  });
  const csvFilesList = csvFilesData?.success ? csvFilesData.files : [];

  // Build full CSV file path from directory + filename
  const csvFilePath = csvDirectory && csvFileName
    ? (csvDirectory.endsWith('/') || csvDirectory.endsWith('\\')
        ? csvDirectory + csvFileName
        : csvDirectory + '/' + csvFileName)
    : csvFileName;

  // Persist CSV directory to localStorage
  useEffect(() => {
    if (csvDirectory) {
      localStorage.setItem('bankImport_csvDirectory', csvDirectory);
    }
  }, [csvDirectory]);

  useEffect(() => {
    if (selectedBankCode) {
      localStorage.setItem('bankImport_bankCode', selectedBankCode);
    }
  }, [selectedBankCode]);

  useEffect(() => {
    if (opera3DataPath) {
      localStorage.setItem('bankImport_opera3DataPath', opera3DataPath);
    }
  }, [opera3DataPath]);

  // Fetch bank accounts using react-query (auto-refreshes on company switch)
  const { data: bankAccountsData } = useQuery({
    queryKey: ['bank-accounts'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/opera-sql/bank-accounts`);
      return res.json();
    },
  });

  // Update bank accounts state when data changes
  useEffect(() => {
    if (bankAccountsData?.success && bankAccountsData.bank_accounts) {
      const accounts = bankAccountsData.bank_accounts.map((b: any) => ({
        code: b.code,
        description: b.description,
        sort_code: b.sort_code || '',
        account_number: b.account_number || ''
      }));
      setBankAccounts(accounts);
      // Only set default if no saved preference
      if (!localStorage.getItem('bankImport_bankCode') && accounts.length > 0) {
        setSelectedBankCode(accounts[0].code);
      }
    }
  }, [bankAccountsData]);


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
  };

  // Bank statement preview with enhanced format detection
  const handleBankPreview = async () => {
    setLoading(true);
    setBankPreview(null);
    setBankImportResult(null);
    setEditedTransactions(new Map());
    setSelectedRows(new Set());
    setIncludedSkipped(new Map());
    setTransactionTypeOverrides(new Map());
    setRefundOverrides(new Map());
    setTabSearchFilter('');
    try {
      let url: string;
      if (dataSource === 'opera-sql') {
        // Use enhanced multi-format preview
        url = `${API_BASE}/bank-import/preview-multiformat?filepath=${encodeURIComponent(csvFilePath)}&bank_code=${selectedBankCode}`;
      } else {
        url = `${API_BASE}/opera3/bank-import/preview?filepath=${encodeURIComponent(csvFilePath)}&data_path=${encodeURIComponent(opera3DataPath)}`;
      }
      const response = await fetch(url, { method: 'POST' });
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

      // Handle enhanced response format
      const enhancedPreview: EnhancedBankImportPreview = {
        success: data.success,
        filename: data.filename,
        detected_format: data.detected_format || 'CSV',
        total_transactions: data.total_transactions,
        matched_receipts: data.matched_receipts || [],
        matched_payments: data.matched_payments || [],
        matched_refunds: data.matched_refunds || [],
        unmatched: data.unmatched || [],
        already_posted: data.already_posted || [],
        skipped: data.skipped || [],
        summary: data.summary,
        errors: data.errors || (data.error ? [data.error] : [])
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
      // Unmatched and skipped - don't pre-select (need manual account assignment first)
      setSelectedForImport(preSelected);

      // Auto-select best tab
      if (enhancedPreview.matched_receipts.length > 0) setActivePreviewTab('receipts');
      else if (enhancedPreview.matched_payments.length > 0) setActivePreviewTab('payments');
      else if (enhancedPreview.matched_refunds?.length > 0) setActivePreviewTab('refunds');
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
        unmatched: [],
        already_posted: [],
        skipped: [],
        errors: [error instanceof Error ? error.message : 'Unknown error']
      });
    } finally {
      setLoading(false);
    }
  };

  // Handle account change for a transaction
  const handleAccountChange = useCallback((txn: BankImportTransaction, accountCode: string, ledgerType: 'C' | 'S') => {
    const updated = new Map(editedTransactions);
    const account = ledgerType === 'C'
      ? customers.find(c => c.code === accountCode)
      : suppliers.find(s => s.code === accountCode);

    updated.set(txn.row, {
      ...txn,
      manual_account: accountCode,
      manual_ledger_type: ledgerType,
      account_name: account?.name || '',
      isEdited: true
    });
    setEditedTransactions(updated);

    // Auto-select for import when account is assigned
    setSelectedForImport(prev => new Set(prev).add(txn.row));
  }, [editedTransactions, customers, suppliers]);

  // Handle row selection for batch operations
  const handleRowSelect = useCallback((row: number, selected: boolean) => {
    const newSelected = new Set(selectedRows);
    if (selected) {
      newSelected.add(row);
    } else {
      newSelected.delete(row);
    }
    setSelectedRows(newSelected);
  }, [selectedRows]);

  // Bulk assign account to selected rows
  const handleBulkAssign = useCallback((accountCode: string, ledgerType: 'C' | 'S') => {
    if (selectedRows.size === 0 || !bankPreview) return;

    const updated = new Map(editedTransactions);
    const account = ledgerType === 'C'
      ? customers.find(c => c.code === accountCode)
      : suppliers.find(s => s.code === accountCode);

    const allUnmatched = bankPreview.unmatched || [];
    allUnmatched.forEach(txn => {
      if (selectedRows.has(txn.row)) {
        updated.set(txn.row, {
          ...txn,
          manual_account: accountCode,
          manual_ledger_type: ledgerType,
          account_name: account?.name || '',
          isEdited: true
        });
      }
    });

    setEditedTransactions(updated);

    // Auto-select for import when accounts are bulk assigned
    setSelectedForImport(prev => {
      const newSet = new Set(prev);
      selectedRows.forEach(row => newSet.add(row));
      return newSet;
    });

    setSelectedRows(new Set());
  }, [selectedRows, editedTransactions, bankPreview, customers, suppliers]);

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

    // Unmatched - selected and have account assigned
    const unmatchedSelected = (bankPreview.unmatched || []).filter(t => selectedForImport.has(t.row));
    const unmatchedWithAccount = unmatchedSelected.filter(t => editedTransactions.get(t.row)?.manual_account);
    const unmatchedReady = unmatchedWithAccount.length;
    const unmatchedIncomplete = unmatchedSelected.length - unmatchedReady; // Selected but missing account
    const unmatchedTotal = (bankPreview.unmatched || []).length;

    // Skipped included - selected (via includedSkipped) and have account assigned
    const skippedIncluded = includedSkipped.size;
    const skippedWithAccount = Array.from(includedSkipped.values()).filter(v => v.account);
    const skippedReady = skippedWithAccount.length;
    const skippedIncomplete = skippedIncluded - skippedReady;

    const totalReady = receiptsReady + paymentsReady + refundsReady + unmatchedReady + skippedReady;
    const totalIncomplete = unmatchedIncomplete + skippedIncomplete; // Items selected but missing account

    return {
      receiptsReady, receiptsTotal,
      paymentsReady, paymentsTotal,
      refundsReady, refundsTotal,
      unmatchedReady, unmatchedTotal, unmatchedIncomplete,
      skippedReady, skippedIncluded, skippedIncomplete,
      totalReady,
      totalIncomplete,
      canImport: totalReady > 0 && totalIncomplete === 0
    };
  })();

  // Bank statement import with manual overrides
  const handleBankImport = async () => {
    setLoading(true);
    setBankImportResult(null);

    try {
      if (dataSource === 'opera3') {
        setBankImportResult({
          success: false,
          error: 'Import not available for Opera 3. Opera 3 data is read-only.'
        });
        setLoading(false);
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

      // Always use import-with-overrides endpoint with selected rows
      const url = `${API_BASE}/bank-import/import-with-overrides?filepath=${encodeURIComponent(csvFilePath)}&bank_code=${selectedBankCode}`;
      const options: RequestInit = {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          overrides,
          selected_rows: selectedRowsArray
        })
      };

      const response = await fetch(url, options);
      const data = await response.json();
      setBankImportResult(data);

      // Clear edited transactions after successful import
      if (data.success) {
        setEditedTransactions(new Map());
        setIncludedSkipped(new Map());
        setTransactionTypeOverrides(new Map());
        setRefundOverrides(new Map());
        setSelectedForImport(new Set());
      }
    } catch (error) {
      setBankImportResult({
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error'
      });
    } finally {
      setLoading(false);
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

      const response = await fetch(`${API_BASE}${endpoint}`, {
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
    { id: 'bank-statement' as ImportType, label: 'Opera Bank Rec', icon: Landmark, color: 'emerald' },
    { id: 'sales-receipt' as ImportType, label: 'Sales Receipt', icon: Receipt, color: 'green' },
    { id: 'purchase-payment' as ImportType, label: 'Purchase Payment', icon: CreditCard, color: 'red' },
    { id: 'sales-invoice' as ImportType, label: 'Sales Invoice', icon: FileText, color: 'blue' },
    { id: 'purchase-invoice' as ImportType, label: 'Purchase Invoice', icon: FileSpreadsheet, color: 'orange' },
    { id: 'nominal-journal' as ImportType, label: 'Nominal Journal', icon: BookOpen, color: 'purple' }
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">{bankRecOnly ? 'Opera Bank Rec' : 'Imports'}</h1>
        <p className="text-gray-600 mt-1">Import and reconcile bank statement transactions</p>
      </div>

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
            Opera Bank Rec
          </h2>

          <div className="space-y-6">
            {/* Data source indicator */}
            <div className="flex items-center gap-2 p-3 bg-gray-50 rounded-lg">
              <span className="text-sm font-medium text-gray-700">Data Source:</span>
              <span className="text-sm font-semibold text-blue-700">
                {dataSource === 'opera-sql' ? 'Opera SQL SE' : 'Opera 3 (FoxPro)'}
              </span>
              <span className="text-xs text-gray-500">(configured in Settings)</span>
            </div>

            {/* Bank Selection (Opera SQL SE) or Data Path (Opera 3) */}
            <div className="grid grid-cols-2 gap-4">
              {dataSource === 'opera-sql' ? (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Bank Account</label>
                  <select
                    value={selectedBankCode}
                    onChange={e => setSelectedBankCode(e.target.value)}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                  >
                    {bankAccounts.map(bank => (
                      <option key={bank.code} value={bank.code}>
                        {bank.code} - {bank.description}
                      </option>
                    ))}
                  </select>
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
                      {csvFilesList.map((f: any) => {
                        const isBankMatch = !f.detected_bank || f.detected_bank === selectedBankCode;
                        const bankIndicator = f.detected_bank
                          ? (isBankMatch ? `[${f.detected_bank}]` : `[${f.detected_bank} - MISMATCH]`)
                          : '';
                        return (
                          <option
                            key={f.filename}
                            value={f.filename}
                            className={!isBankMatch ? 'text-red-600' : ''}
                          >
                            {f.filename} — {f.modified} ({f.size_display}) {bankIndicator}
                          </option>
                        );
                      })}
                    </select>
                  ) : (
                    <input
                      type="text"
                      value={csvFileName}
                      onChange={e => setCsvFileName(e.target.value)}
                      className="w-full px-3 py-2 bg-white border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      placeholder={csvDirectory ? 'No CSV files found in folder' : 'Enter folder path first'}
                    />
                  )}
                  {/* Bank mismatch warning */}
                  {csvFileName && csvFilesList && (() => {
                    const selectedFile = csvFilesList.find((f: any) => f.filename === csvFileName);
                    if (selectedFile?.detected_bank && selectedFile.detected_bank !== selectedBankCode) {
                      return (
                        <div className="mt-2 p-2 bg-amber-50 border border-amber-300 rounded-md flex items-start gap-2">
                          <AlertCircle className="h-4 w-4 text-amber-600 mt-0.5 flex-shrink-0" />
                          <div className="text-sm text-amber-800">
                            <span className="font-medium">Bank mismatch:</span> This CSV file is for bank account <strong>{selectedFile.detected_bank}</strong>,
                            but you have selected <strong>{selectedBankCode}</strong>. Please select the correct bank account or choose a different CSV file.
                          </div>
                        </div>
                      );
                    }
                    return null;
                  })()}
                </div>
              </div>
            </div>

            {/* Preview / Import Buttons */}
            {(() => {
              // Check for bank mismatch
              const selectedFile = csvFilesList?.find((f: any) => f.filename === csvFileName);
              const hasBankMismatch = selectedFile?.detected_bank && selectedFile.detected_bank !== selectedBankCode;

              // Determine if import is allowed
              const hasIncomplete = importReadiness?.totalIncomplete && importReadiness.totalIncomplete > 0;
              const hasNothingToImport = importReadiness && importReadiness.totalReady === 0;
              const importDisabled = loading || dataSource === 'opera3' || hasBankMismatch || hasIncomplete || hasNothingToImport;

              // Build tooltip message
              let importTitle = '';
              if (hasBankMismatch) importTitle = 'Cannot import - bank account mismatch';
              else if (dataSource === 'opera3') importTitle = 'Import not available for Opera 3 (read-only)';
              else if (hasIncomplete) importTitle = 'Cannot import - some included items are missing required account assignment';
              else if (hasNothingToImport) importTitle = 'No transactions ready to import';

              return (
                <div className="space-y-3">
                  <div className="flex gap-4">
                    <button
                      onClick={handleBankPreview}
                      disabled={loading || hasBankMismatch}
                      className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center gap-2"
                      title={hasBankMismatch ? 'Cannot preview - bank account mismatch' : ''}
                    >
                      {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
                      Preview Import
                    </button>
                    <button
                      onClick={handleBankImport}
                      disabled={importDisabled}
                      className="px-6 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center gap-2"
                      title={importTitle}
                    >
                      {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckCircle className="h-4 w-4" />}
                      Import Transactions
                      {importReadiness && importReadiness.totalReady > 0 && (
                        <span className="bg-green-500 text-white text-xs px-1.5 py-0.5 rounded-full ml-1">
                          {importReadiness.totalReady}
                        </span>
                      )}
                    </button>
                  </div>

                  {/* Import Readiness Summary */}
                  {importReadiness && bankPreview && (
                    <div className={`p-3 rounded-lg text-sm ${
                      hasIncomplete ? 'bg-red-50 border border-red-200' :
                      importReadiness.totalReady > 0 ? 'bg-green-50 border border-green-200' :
                      'bg-gray-50 border border-gray-200'
                    }`}>
                      <div className="flex flex-wrap items-center gap-3">
                        <span className="font-medium">
                          {hasIncomplete ? (
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
                        {hasIncomplete && (
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
                    {bankPreview.detected_format && (
                      <span className="text-xs px-2 py-1 bg-blue-100 text-blue-700 rounded-full">
                        Format: {bankPreview.detected_format}
                      </span>
                    )}
                  </div>

                  {/* Transaction Totals Summary */}
                  {(() => {
                    const receiptsCount = bankPreview.matched_receipts?.length || 0;
                    const paymentsCount = bankPreview.matched_payments?.length || 0;
                    const refundsCount = bankPreview.matched_refunds?.length || 0;
                    const unmatchedCount = bankPreview.unmatched?.length || 0;
                    const alreadyPostedCount = bankPreview.already_posted?.length || 0;
                    const skippedCount = bankPreview.skipped?.length || 0;
                    const totalInStatement = receiptsCount + paymentsCount + refundsCount + unmatchedCount + alreadyPostedCount + skippedCount;
                    const selectedCount = selectedForImport.size;

                    return (
                      <div className="mb-3 p-3 bg-white border border-blue-200 rounded-lg">
                        <div className="flex flex-wrap items-center gap-4 text-sm">
                          <div className="font-semibold text-gray-900">
                            Statement Total: <span className="text-blue-700">{totalInStatement}</span> transactions
                          </div>
                          <div className="text-gray-500">|</div>
                          <div className="flex flex-wrap gap-3 text-xs">
                            <span className="text-green-700">{receiptsCount} receipts</span>
                            <span className="text-red-700">{paymentsCount} payments</span>
                            {refundsCount > 0 && <span className="text-orange-700">{refundsCount} refunds</span>}
                            <span className="text-amber-700">{unmatchedCount} unmatched</span>
                            <span className="text-gray-500">{alreadyPostedCount + skippedCount} already posted/skipped</span>
                          </div>
                          <div className="text-gray-500">|</div>
                          <div className={`font-medium ${selectedCount > 0 ? 'text-green-700' : 'text-gray-500'}`}>
                            {selectedCount} selected for import
                          </div>
                        </div>
                      </div>
                    );
                  })()}

                  {/* Tab Bar / Summary */}
                  <div className="flex flex-wrap gap-2">
                    <button
                      onClick={() => { setActivePreviewTab('receipts'); setTabSearchFilter(''); }}
                      className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                        activePreviewTab === 'receipts'
                          ? 'bg-green-100 text-green-800 border-2 border-green-400'
                          : 'bg-green-50 text-green-700 border border-green-200 hover:bg-green-100'
                      }`}
                    >
                      Receipts
                      <span className="bg-green-200 text-green-900 px-1.5 py-0.5 rounded-full text-xs font-bold">
                        {bankPreview.matched_receipts?.length || 0}
                      </span>
                    </button>
                    <button
                      onClick={() => { setActivePreviewTab('payments'); setTabSearchFilter(''); }}
                      className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                        activePreviewTab === 'payments'
                          ? 'bg-red-100 text-red-800 border-2 border-red-400'
                          : 'bg-red-50 text-red-700 border border-red-200 hover:bg-red-100'
                      }`}
                    >
                      Payments
                      <span className="bg-red-200 text-red-900 px-1.5 py-0.5 rounded-full text-xs font-bold">
                        {bankPreview.matched_payments?.length || 0}
                      </span>
                    </button>
                    {(bankPreview.matched_refunds?.length || 0) > 0 && (
                      <button
                        onClick={() => { setActivePreviewTab('refunds'); setTabSearchFilter(''); }}
                        className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                          activePreviewTab === 'refunds'
                            ? 'bg-orange-100 text-orange-800 border-2 border-orange-400'
                            : 'bg-orange-50 text-orange-700 border border-orange-200 hover:bg-orange-100'
                        }`}
                      >
                        Refunds
                        <span className="bg-orange-200 text-orange-900 px-1.5 py-0.5 rounded-full text-xs font-bold">
                          {bankPreview.matched_refunds?.length || 0}
                        </span>
                      </button>
                    )}
                    <button
                      onClick={() => { setActivePreviewTab('unmatched'); setTabSearchFilter(''); }}
                      className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                        activePreviewTab === 'unmatched'
                          ? 'bg-amber-100 text-amber-800 border-2 border-amber-400'
                          : 'bg-amber-50 text-amber-700 border border-amber-200 hover:bg-amber-100'
                      }`}
                    >
                      Unmatched
                      <span className="bg-amber-200 text-amber-900 px-1.5 py-0.5 rounded-full text-xs font-bold">
                        {bankPreview.unmatched?.length || 0}
                      </span>
                      {editedTransactions.size > 0 && (
                        <span className="text-green-600 text-xs">({editedTransactions.size} assigned)</span>
                      )}
                    </button>
                    <button
                      onClick={() => { setActivePreviewTab('skipped'); setTabSearchFilter(''); }}
                      className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                        activePreviewTab === 'skipped'
                          ? 'bg-gray-200 text-gray-800 border-2 border-gray-400'
                          : 'bg-gray-50 text-gray-600 border border-gray-200 hover:bg-gray-100'
                      }`}
                    >
                      Skipped
                      <span className="bg-gray-200 text-gray-800 px-1.5 py-0.5 rounded-full text-xs font-bold">
                        {(bankPreview.already_posted?.length || 0) + (bankPreview.skipped?.length || 0)}
                      </span>
                      {includedSkipped.size > 0 && (
                        <span className="text-green-600 text-xs">({includedSkipped.size} included)</span>
                      )}
                    </button>
                    <div className="ml-auto text-sm text-gray-500 self-center">
                      Total: {bankPreview.total_transactions}
                    </div>
                  </div>
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
                      <div className="overflow-x-auto max-h-96 overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0">
                            <tr className="bg-green-100">
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
                                <td className="p-2">{txn.date}</td>
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
                      <div className="overflow-x-auto max-h-96 overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0">
                            <tr className="bg-red-100">
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
                                <td className="p-2">{txn.date}</td>
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
                      <div className="overflow-x-auto max-h-96 overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0">
                            <tr className="bg-orange-100">
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
                              const currentAccount = override?.account || txn.account;
                              const isModified = override && (override.transaction_type || override.account);
                              const isSelected = selectedForImport.has(txn.row);
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
                                  <td className="p-2">{txn.date}</td>
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
                                      }}
                                      className={`text-xs px-2 py-1 border rounded bg-white w-full ${
                                        override?.transaction_type ? 'border-yellow-400 bg-yellow-50' : 'border-gray-300'
                                      }`}
                                    >
                                      <option value="sales_refund">Sales Refund</option>
                                      <option value="purchase_refund">Purchase Refund</option>
                                      <option value="sales_receipt">Sales Receipt</option>
                                      <option value="purchase_payment">Purchase Payment</option>
                                    </select>
                                  </td>
                                  <td className="p-2">
                                    <select
                                      value={override?.account ? `${override.ledger_type}:${override.account}` : (txn.account ? `${showCustomers ? 'C' : 'S'}:${txn.account}` : '')}
                                      onChange={(e) => {
                                        const [type, code] = e.target.value.split(':');
                                        if (code) {
                                          const updated = new Map(refundOverrides);
                                          const current = updated.get(txn.row) || {};
                                          updated.set(txn.row, {
                                            ...current,
                                            account: code,
                                            ledger_type: type as 'C' | 'S'
                                          });
                                          setRefundOverrides(updated);
                                        }
                                      }}
                                      className={`w-full text-xs px-2 py-1 border rounded ${
                                        override?.account ? 'border-yellow-400 bg-yellow-50' : 'border-gray-300'
                                      }`}
                                    >
                                      <option value={txn.account ? `${showCustomers ? 'C' : 'S'}:${txn.account}` : ''}>
                                        {currentAccount} - {txn.account_name || '(matched)'}
                                      </option>
                                      <optgroup label={showCustomers ? 'Customers' : 'Suppliers'}>
                                        {(showCustomers ? customers : suppliers).map(acc => (
                                          <option key={`${showCustomers ? 'C' : 'S'}:${acc.code}`} value={`${showCustomers ? 'C' : 'S'}:${acc.code}`}>
                                            {acc.code} - {acc.name}
                                          </option>
                                        ))}
                                      </optgroup>
                                    </select>
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
                      <div className="overflow-x-auto max-h-96 overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-amber-100">
                            <tr>
                              <th className="p-2 text-left w-16">Include</th>
                              <th className="text-left p-2">Date</th>
                              <th className="text-left p-2">Name</th>
                              <th className="text-right p-2">Amount</th>
                              <th className="text-left p-2">Transaction Type</th>
                              <th className="text-left p-2 min-w-[200px]">Assign Account</th>
                              <th className="text-left p-2">Status</th>
                            </tr>
                          </thead>
                          <tbody>
                            {filtered.map((txn) => {
                              const editedTxn = editedTransactions.get(txn.row);
                              const isPositive = txn.amount > 0;
                              const currentTxnType = transactionTypeOverrides.get(txn.row) || (isPositive ? 'sales_receipt' : 'purchase_payment');
                              const showCustomers = currentTxnType === 'sales_receipt' || currentTxnType === 'sales_refund';
                              const isIncluded = selectedForImport.has(txn.row);
                              const hasAccount = editedTxn?.manual_account;
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
                                  <td className="p-2">{txn.date}</td>
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
                                      }}
                                      className="text-xs px-2 py-1 border border-gray-300 rounded bg-white w-full"
                                    >
                                      <option value="sales_receipt">Sales Receipt</option>
                                      <option value="purchase_payment">Purchase Payment</option>
                                      <option value="sales_refund">Sales Refund</option>
                                      <option value="purchase_refund">Purchase Refund</option>
                                    </select>
                                  </td>
                                  <td className="p-2">
                                    <select
                                      value={editedTxn?.manual_account ? `${editedTxn.manual_ledger_type}:${editedTxn.manual_account}` : ''}
                                      onChange={(e) => {
                                        const [type, code] = e.target.value.split(':');
                                        if (code) handleAccountChange(txn, code, type as 'C' | 'S');
                                      }}
                                      className={`w-full text-sm px-2 py-1 border rounded ${
                                        editedTxn?.isEdited ? 'border-green-400 bg-green-50' : 'border-gray-300'
                                      }`}
                                    >
                                      <option value="">-- Select Account --</option>
                                      {showCustomers ? (
                                        <optgroup label="Customers">
                                          {customers.map(c => (
                                            <option key={`C:${c.code}`} value={`C:${c.code}`}>{c.code} - {c.name}</option>
                                          ))}
                                        </optgroup>
                                      ) : (
                                        <optgroup label="Suppliers">
                                          {suppliers.map(s => (
                                            <option key={`S:${s.code}`} value={`S:${s.code}`}>{s.code} - {s.name}</option>
                                          ))}
                                        </optgroup>
                                      )}
                                    </select>
                                  </td>
                                  <td className="p-2">
                                    {editedTxn?.isEdited ? (
                                      <span className="inline-flex items-center gap-1 text-green-600 text-xs">
                                        <CheckCircle className="h-3 w-3" /> Ready
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
                      <div className="overflow-x-auto max-h-96 overflow-y-auto">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0">
                            <tr className="bg-gray-100">
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
                              const skippedTxnType = inclusion?.transaction_type || (isPositive ? 'sales_receipt' : 'purchase_payment');
                              const showCust = skippedTxnType === 'sales_receipt' || skippedTxnType === 'sales_refund';
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
                                            updated.set(txn.row, {
                                              account: '',
                                              ledger_type: isPositive ? 'C' : 'S',
                                              transaction_type: isPositive ? 'sales_receipt' : 'purchase_payment'
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
                                  <td className="p-2">{txn.date}</td>
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
                                        }}
                                        className="text-xs px-2 py-1 border border-gray-300 rounded bg-white w-full"
                                      >
                                        <option value="sales_receipt">Sales Receipt</option>
                                        <option value="purchase_payment">Purchase Payment</option>
                                        <option value="sales_refund">Sales Refund</option>
                                        <option value="purchase_refund">Purchase Refund</option>
                                      </select>
                                    )}
                                  </td>
                                  <td className="p-2">
                                    {isIncluded && (
                                      <select
                                        value={inclusion?.account ? `${inclusion.ledger_type}:${inclusion.account}` : ''}
                                        onChange={(e) => {
                                          const [type, code] = e.target.value.split(':');
                                          if (code) {
                                            const updated = new Map(includedSkipped);
                                            const current = updated.get(txn.row)!;
                                            updated.set(txn.row, { ...current, account: code, ledger_type: type as 'C' | 'S' });
                                            setIncludedSkipped(updated);
                                          }
                                        }}
                                        className={`w-full text-sm px-2 py-1 border rounded ${
                                          inclusion?.account ? 'border-green-400 bg-green-50' : 'border-gray-300'
                                        }`}
                                      >
                                        <option value="">-- Select Account --</option>
                                        {showCust ? (
                                          <optgroup label="Customers">
                                            {customers.map(c => (
                                              <option key={`C:${c.code}`} value={`C:${c.code}`}>{c.code} - {c.name}</option>
                                            ))}
                                          </optgroup>
                                        ) : (
                                          <optgroup label="Suppliers">
                                            {suppliers.map(s => (
                                              <option key={`S:${s.code}`} value={`S:${s.code}`}>{s.code} - {s.name}</option>
                                            ))}
                                          </optgroup>
                                        )}
                                      </select>
                                    )}
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
                {bankImportResult.imported_count !== undefined && (
                  <div className="text-sm text-gray-700">
                    <p className="font-medium">
                      Imported {bankImportResult.imported_count} transactions
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
                  <p className="text-sm text-red-600">{bankImportResult.error}</p>
                )}
                {bankImportResult.errors && bankImportResult.errors.length > 0 && (
                  <ul className="mt-2 list-disc list-inside text-sm text-red-600">
                    {bankImportResult.errors.map((err: any, idx: number) => (
                      <li key={idx}>Row {err.row}: {err.error}</li>
                    ))}
                  </ul>
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
                      <li>Click "Preview Import" to analyze the bank statement</li>
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
              disabled={loading}
              className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center gap-2"
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
    </div>
  );
}
