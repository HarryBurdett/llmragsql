import { useState, useCallback, useMemo, useEffect } from 'react';
import { Landmark, RefreshCw, FileText, ArrowRight, CheckCircle, AlertTriangle, Search, ChevronDown, ChevronRight, Mail, FolderOpen, X, Archive, Trash2, Eye, Clock } from 'lucide-react';
import { authFetch } from '../api/client';
import { Imports } from './Imports';
import { BankStatementReconcile } from './BankStatementReconcile';

// ---- Types ----

interface StatementEntry {
  email_id?: number;
  attachment_id?: string;
  filename: string;
  source: 'email' | 'pdf';
  full_path?: string;
  folder?: string;
  subject?: string;
  from_address?: string;
  received_at?: string;
  detected_bank_name?: string;
  already_processed?: boolean;
  is_reconciled?: boolean;
  is_imported?: boolean;
  status: 'ready' | 'sequence_gap' | 'uncached' | 'pending' | 'already_processed' | 'imported';
  validation_note?: string;
  opening_balance?: number;
  closing_balance?: number;
  period_start?: string;
  period_end?: string;
  bank_name?: string;
  account_number?: string;
  sort_code?: string;
  import_sequence?: number;
  statement_date?: string;
  category?: 'already_processed' | 'old_statement' | 'not_classified' | 'advanced';
  matched_bank_code?: string;
  matched_bank_description?: string;
  matched_sort_code?: string;
  matched_account_number?: string;
  balance_gap?: number;
}

interface BankGroup {
  bank_code: string;
  description: string;
  sort_code: string;
  account_number: string;
  reconciled_balance: number | null;
  current_balance: number | null;
  type: string;
  statements: StatementEntry[];
  statement_count: number;
}

interface NonCurrentStatements {
  already_processed: StatementEntry[];
  old_statements: StatementEntry[];
  not_classified: StatementEntry[];
  advanced: StatementEntry[];
}

interface ScanResult {
  success: boolean;
  banks: Record<string, BankGroup>;
  unidentified: StatementEntry[];
  non_current: NonCurrentStatements;
  non_current_count: number;
  total_statements: number;
  total_banks_with_statements: number;
  total_banks_loaded: number;
  total_emails_scanned: number;
  total_pdfs_found: number;
  duplicates_archived: number;
  days_searched: number;
  message: string;
  error?: string;
}

interface InProgressStatement {
  id: number;
  filename: string;
  bank_code: string;
  source: string;
  transactions_imported: number;
  total_receipts: number;
  total_payments: number;
  import_date: string;
  imported_by: string;
  target_system: string;
  email_id?: number;
  attachment_id?: string;
  opening_balance?: number;
  closing_balance?: number;
  statement_date?: string;
  account_number?: string;
  sort_code?: string;
  stored_transaction_count: number;
}

type TabType = 'pending' | 'in_progress' | 'manage' | 'process' | 'reconcile';

interface ReconcileHandoff {
  bank_code: string;
  statement_transactions: any[];
  statement_info: any;
  source: string;
  filename?: string;
  import_id?: number;
}

// ---- Component ----

export function BankStatementHub() {
  const [activeTab, setActiveTab] = useState<TabType>('pending');
  const [scanResult, setScanResult] = useState<ScanResult | null>(null);
  const [scanning, setScanning] = useState(false);
  const [scanError, setScanError] = useState<string | null>(null);
  const [lastScanTime, setLastScanTime] = useState<string | null>(null);
  const [daysBack, setDaysBack] = useState(30);

  const [selectedStatement, setSelectedStatement] = useState<{
    bankCode: string;
    bankDescription: string;
    statement: StatementEntry;
  } | null>(null);

  const [reconcileData, setReconcileData] = useState<ReconcileHandoff | null>(null);
  const [resumeStatement, setResumeStatement] = useState<InProgressStatement | null>(null);
  const [resumeImportId, setResumeImportId] = useState<number | null>(null);
  const [inProgressStatements, setInProgressStatements] = useState<InProgressStatement[]>([]);
  const [inProgressLoading, setInProgressLoading] = useState(false);
  const [expandedBanks, setExpandedBanks] = useState<Set<string>>(new Set());

  const nonCurrentCount = scanResult?.non_current_count || 0;

  const fetchInProgress = useCallback(async () => {
    setInProgressLoading(true);
    try {
      const resp = await authFetch('/api/statement-files/imported-for-reconciliation');
      const data = await resp.json();
      if (data.success) setInProgressStatements(data.statements || []);
    } catch (err) {
      console.error('Failed to fetch in-progress statements:', err);
    } finally {
      setInProgressLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchInProgress();
  }, [fetchInProgress]);

  const handleScan = useCallback(async () => {
    setScanning(true);
    setScanError(null);
    try {
      const resp = await authFetch(`/api/bank-import/scan-all-banks?days_back=${daysBack}&validate_balances=true`);
      const data: ScanResult = await resp.json();
      if (data.success) {
        setScanResult(data);
        setLastScanTime(new Date().toLocaleTimeString());
        setExpandedBanks(new Set(Object.keys(data.banks)));
      } else {
        setScanError(data.error || 'Scan failed');
      }
    } catch (err: any) {
      setScanError(err.message || 'Network error');
    } finally {
      setScanning(false);
      fetchInProgress();
    }
  }, [daysBack, fetchInProgress]);

  const handleProcess = useCallback((bankCode: string, bankDescription: string, stmt: StatementEntry) => {
    setSelectedStatement({ bankCode, bankDescription, statement: stmt });
    setReconcileData(null);
    setActiveTab('process');
  }, []);

  const handleImportComplete = useCallback((data: ReconcileHandoff) => {
    setReconcileData(data);
    setResumeStatement(null);
    setActiveTab('reconcile');
    fetchInProgress(); // Refresh in-progress list after new import
  }, [fetchInProgress]);

  const handleReconcileComplete = useCallback(() => {
    setSelectedStatement(null);
    setReconcileData(null);
    setResumeStatement(null);
    setActiveTab('pending');
    handleScan();
    fetchInProgress();
  }, [handleScan, fetchInProgress]);

  const handleResumeReconcile = useCallback((stmt: InProgressStatement) => {
    setResumeStatement(stmt);
    setReconcileData(null);
    setSelectedStatement(null);
    setActiveTab('reconcile');
  }, []);

  const handleReprocessStatement = useCallback(async (stmt: InProgressStatement) => {
    if (!window.confirm(
      `Reprocess: ${stmt.filename}\n\n` +
      `This will:\n` +
      `• Clear the import tracking record (${stmt.transactions_imported} of ${stmt.stored_transaction_count} transactions imported)\n` +
      `• Remove stored statement transactions from the local database\n` +
      `• Allow you to re-import this statement from scratch\n\n` +
      `This does NOT affect Opera cashbook entries already posted.\n\n` +
      `Continue?`
    )) {
      return;
    }
    // Delete import tracking data (does not affect Opera cashbook entries)
    try {
      const resp = await authFetch(`/api/bank-import/import-history/${stmt.id}`, { method: 'DELETE' });
      const data = await resp.json();
      if (!data.success) {
        alert(`Failed to reset statement: ${data.error || 'Unknown error'}`);
        return;
      }
    } catch (err) {
      alert(`Failed to reset statement: ${err}`);
      return;
    }
    // Refresh in-progress list (await to update badge before navigating)
    await fetchInProgress();
    // Map DB source values to what Imports component expects
    const source: 'email' | 'pdf' = stmt.source === 'email' ? 'email' : 'pdf';
    const stmtEntry: StatementEntry = {
      email_id: stmt.email_id,
      attachment_id: stmt.attachment_id,
      filename: stmt.filename,
      source,
      status: 'ready',
      is_imported: false,
      opening_balance: stmt.opening_balance,
      closing_balance: stmt.closing_balance,
      statement_date: stmt.statement_date,
      account_number: stmt.account_number,
      sort_code: stmt.sort_code,
    };
    setSelectedStatement({ bankCode: stmt.bank_code, bankDescription: stmt.bank_code, statement: stmtEntry });
    setReconcileData(null);
    setResumeStatement(null);
    setActiveTab('process');
  }, [fetchInProgress]);

  const handleContinueImport = useCallback((stmt: InProgressStatement) => {
    // Look up full path from scan results if available
    let fullPath: string | undefined;
    if (scanResult?.banks) {
      const bankGroup = scanResult.banks[stmt.bank_code];
      if (bankGroup) {
        const match = bankGroup.statements.find(s => s.filename === stmt.filename);
        if (match?.full_path) fullPath = match.full_path;
      }
    }

    const source: 'email' | 'pdf' = stmt.source === 'email' ? 'email' : 'pdf';
    const stmtEntry: StatementEntry = {
      email_id: stmt.email_id,
      attachment_id: stmt.attachment_id,
      filename: stmt.filename,
      source,
      full_path: fullPath,
      status: 'ready',
      is_imported: false,
      opening_balance: stmt.opening_balance,
      closing_balance: stmt.closing_balance,
      statement_date: stmt.statement_date,
      account_number: stmt.account_number,
      sort_code: stmt.sort_code,
    };
    setSelectedStatement({ bankCode: stmt.bank_code, bankDescription: stmt.bank_code, statement: stmtEntry });
    setReconcileData(null);
    setResumeStatement(null);
    setResumeImportId(stmt.id);
    setActiveTab('process');
  }, [scanResult]);

  const handleReconcileFromPending = useCallback((bankCode: string, stmt: StatementEntry) => {
    const match = inProgressStatements.find(ip =>
      ip.filename === stmt.filename && ip.bank_code === bankCode
    );
    if (match) {
      handleResumeReconcile(match);
    }
  }, [inProgressStatements, handleResumeReconcile]);

  const handleBackToPending = useCallback(() => {
    setResumeImportId(null);
    setActiveTab('pending');
  }, []);

  const toggleBank = useCallback((code: string) => {
    setExpandedBanks(prev => {
      const next = new Set(prev);
      if (next.has(code)) next.delete(code);
      else next.add(code);
      return next;
    });
  }, []);

  const bankList = useMemo(() => {
    if (!scanResult?.banks) return [];
    return Object.values(scanResult.banks).sort((a, b) => a.bank_code.localeCompare(b.bank_code));
  }, [scanResult]);

  const tabs: { key: TabType; label: string; disabled: boolean; badge?: number }[] = [
    { key: 'pending', label: 'Load Statements', disabled: false, badge: scanResult?.total_statements },
    { key: 'process', label: 'Process & Import', disabled: !selectedStatement },
    { key: 'in_progress', label: 'In Progress', disabled: false, badge: inProgressStatements.length || undefined },
    { key: 'reconcile', label: 'Reconcile', disabled: !reconcileData && !resumeStatement },
    { key: 'manage', label: 'Manage', disabled: !scanResult || nonCurrentCount === 0, badge: nonCurrentCount || undefined },
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Landmark className="h-6 w-6 text-blue-600" />
          <h1 className="text-xl font-semibold text-gray-900">Bank Statements</h1>
        </div>
      </div>

      <div className="flex border-b border-gray-200">
        {tabs.map(tab => (
          <button
            key={tab.key}
            onClick={() => !tab.disabled && setActiveTab(tab.key)}
            disabled={tab.disabled}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
              activeTab === tab.key
                ? 'border-blue-600 text-blue-600'
                : tab.disabled
                  ? 'border-transparent text-gray-300 cursor-not-allowed'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
            }`}
          >
            {tab.label}
            {tab.badge != null && tab.badge > 0 && (
              <span className={`ml-1.5 text-xs rounded-full px-1.5 py-0.5 ${
                tab.key === 'manage' ? 'bg-amber-100 text-amber-700' : 'bg-blue-100 text-blue-700'
              }`}>
                {tab.badge}
              </span>
            )}
          </button>
        ))}
      </div>

      {activeTab === 'pending' && (
        <PendingStatementsTab
          scanResult={scanResult}
          bankList={bankList}
          scanning={scanning}
          scanError={scanError}
          lastScanTime={lastScanTime}
          daysBack={daysBack}
          setDaysBack={setDaysBack}
          expandedBanks={expandedBanks}
          toggleBank={toggleBank}
          nonCurrentCount={nonCurrentCount}
          onScan={handleScan}
          onProcess={handleProcess}
          onReconcile={handleReconcileFromPending}
          onSwitchToManage={() => setActiveTab('manage')}
        />
      )}

      {activeTab === 'in_progress' && (
        <InProgressTab
          statements={inProgressStatements}
          loading={inProgressLoading}
          onResume={handleResumeReconcile}
          onReprocess={handleReprocessStatement}
          onContinueImport={handleContinueImport}
          onRefresh={fetchInProgress}
        />
      )}

      {activeTab === 'manage' && scanResult && (
        <ManageStatementsTab
          nonCurrent={scanResult.non_current}
          onRefresh={handleScan}
        />
      )}

      {activeTab === 'process' && selectedStatement && (
        <div>
          <div className="mb-3 flex items-center gap-2">
            <button onClick={handleBackToPending} className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1">
              <ArrowRight className="h-3.5 w-3.5 rotate-180" /> Back to Pending
            </button>
            <span className="text-gray-300">|</span>
            <span className="text-sm text-gray-600">
              {resumeImportId ? 'Continue Import' : 'Processing'}: <strong>{selectedStatement.statement.filename}</strong> for {selectedStatement.bankDescription}
            </span>
            {resumeImportId && (
              <span className="px-2 py-0.5 text-xs font-medium bg-orange-100 text-orange-700 rounded-full">
                Resume — already-posted lines will be skipped
              </span>
            )}
          </div>
          <Imports
            key={`${selectedStatement.bankCode}-${selectedStatement.statement.filename}-${selectedStatement.statement.email_id || ''}-${resumeImportId || ''}`}
            bankRecOnly
            initialStatement={{
              bankCode: selectedStatement.bankCode,
              bankDescription: selectedStatement.bankDescription,
              emailId: selectedStatement.statement.email_id,
              attachmentId: selectedStatement.statement.attachment_id,
              filename: selectedStatement.statement.filename,
              source: selectedStatement.statement.source,
              fullPath: selectedStatement.statement.full_path,
            }}
            resumeImportId={resumeImportId || undefined}
            onImportComplete={(data) => {
              setResumeImportId(null);
              handleImportComplete(data);
            }}
          />
        </div>
      )}

      {activeTab === 'reconcile' && (reconcileData || resumeStatement) && (
        <div>
          <div className="mb-3 flex items-center gap-2">
            <button onClick={handleBackToPending} className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1">
              <ArrowRight className="h-3.5 w-3.5 rotate-180" /> Back to Pending
            </button>
            <span className="text-gray-300">|</span>
            <span className="text-sm text-gray-600">
              Reconciling: <strong>{reconcileData?.filename || resumeStatement?.filename || 'Statement'}</strong>
            </span>
          </div>
          <BankStatementReconcile
            initialReconcileData={reconcileData}
            resumeImportId={resumeStatement?.id}
            resumeStatement={resumeStatement ? {
              id: resumeStatement.id,
              bank_code: resumeStatement.bank_code,
              filename: resumeStatement.filename,
              source: resumeStatement.source,
              opening_balance: resumeStatement.opening_balance,
              closing_balance: resumeStatement.closing_balance,
              statement_date: resumeStatement.statement_date,
            } : undefined}
            onReconcileComplete={handleReconcileComplete}
          />
        </div>
      )}
    </div>
  );
}

// ---- Pending Statements Tab ----

function PendingStatementsTab({
  scanResult, bankList, scanning, scanError, lastScanTime, daysBack, setDaysBack,
  expandedBanks, toggleBank, nonCurrentCount, onScan, onProcess, onReconcile, onSwitchToManage,
}: {
  scanResult: ScanResult | null;
  bankList: BankGroup[];
  scanning: boolean;
  scanError: string | null;
  lastScanTime: string | null;
  daysBack: number;
  setDaysBack: (d: number) => void;
  expandedBanks: Set<string>;
  toggleBank: (code: string) => void;
  nonCurrentCount: number;
  onScan: () => void;
  onProcess: (bankCode: string, bankDescription: string, stmt: StatementEntry) => void;
  onReconcile: (bankCode: string, stmt: StatementEntry) => void;
  onSwitchToManage: () => void;
}) {
  return (
    <div className="space-y-4">
      <div className="bg-white border border-gray-200 rounded-lg p-4">
        <div className="flex items-center justify-between flex-wrap gap-3">
          <div className="flex items-center gap-3">
            <button onClick={onScan} disabled={scanning}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2 text-sm font-medium">
              {scanning ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
              {scanning ? 'Scanning...' : 'Scan All Banks'}
            </button>
            <div className="flex items-center gap-1.5 text-sm text-gray-600">
              <span>Last</span>
              <select value={daysBack} onChange={e => setDaysBack(Number(e.target.value))}
                className="border border-gray-300 rounded px-2 py-1 text-sm">
                <option value={7}>7 days</option>
                <option value={14}>14 days</option>
                <option value={30}>30 days</option>
                <option value={60}>60 days</option>
                <option value={90}>90 days</option>
              </select>
            </div>
          </div>
          {lastScanTime && <span className="text-xs text-gray-400">Last scan: {lastScanTime}</span>}
        </div>
        {scanResult && !scanning && (
          <div className="mt-3 text-sm text-gray-600">
            {scanResult.message}
            {scanResult.duplicates_archived > 0 && (
              <span className="ml-2 inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-700">
                Auto-archived {scanResult.duplicates_archived} duplicate{scanResult.duplicates_archived !== 1 ? 's' : ''}
              </span>
            )}
          </div>
        )}
      </div>

      {scanError && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
          <span className="text-red-500">⚠</span>
          <div className="flex-1">
            <p className="text-sm text-red-800 font-medium">Scan Error</p>
            <p className="text-sm text-red-700">{scanError}</p>
          </div>
        </div>
      )}

      {!scanResult && !scanning && !scanError && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <Landmark className="h-10 w-10 text-gray-300 mx-auto mb-3" />
          <p className="text-gray-500 text-sm">Click "Scan All Banks" to find pending statements across all bank accounts</p>
        </div>
      )}

      {scanning && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
          <RefreshCw className="h-8 w-8 text-blue-400 mx-auto mb-2 animate-spin" />
          <p className="text-blue-700 text-sm font-medium">Scanning emails and files for bank statements...</p>
          <p className="text-blue-500 text-xs mt-1">Checking PDF cache for account matching</p>
        </div>
      )}

      {scanResult && !scanning && bankList.length > 0 && (
        <div className="space-y-3">
          {bankList.map(bank => (
            <BankCard key={bank.bank_code} bank={bank}
              expanded={expandedBanks.has(bank.bank_code)}
              onToggle={() => toggleBank(bank.bank_code)}
              onProcess={(stmt) => onProcess(bank.bank_code, bank.description, stmt)}
              onReconcile={(stmt) => onReconcile(bank.bank_code, stmt)} />
          ))}
        </div>
      )}

      {scanResult && !scanning && bankList.length === 0 && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-6 text-center">
          <CheckCircle className="h-8 w-8 text-green-400 mx-auto mb-2" />
          <p className="text-green-700 text-sm font-medium">All bank statements are up to date</p>
          <p className="text-green-600 text-xs mt-1">No pending statements found across {scanResult.total_banks_loaded} bank accounts</p>
        </div>
      )}

      {/* Non-current link (replaces old unidentified box) */}
      {scanResult && nonCurrentCount > 0 && !scanning && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm text-gray-600">
            <AlertTriangle className="h-4 w-4 text-amber-500" />
            <span>{nonCurrentCount} non-current statement{nonCurrentCount !== 1 ? 's' : ''} found (already processed, old, or unmatched)</span>
          </div>
          <button onClick={onSwitchToManage}
            className="text-sm text-blue-600 hover:text-blue-800 font-medium flex items-center gap-1">
            Manage <ArrowRight className="h-3.5 w-3.5" />
          </button>
        </div>
      )}
    </div>
  );
}

// ---- In Progress Tab ----

function InProgressTab({
  statements, loading, onResume, onReprocess, onContinueImport, onRefresh,
}: {
  statements: InProgressStatement[];
  loading: boolean;
  onResume: (stmt: InProgressStatement) => void;
  onReprocess: (stmt: InProgressStatement) => void;
  onContinueImport: (stmt: InProgressStatement) => void;
  onRefresh: () => void;
}) {
  const formatBal = (val: number | undefined | null) => {
    if (val === null || val === undefined) return '—';
    return `£${val.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const formatDate = (dateStr: string) => {
    if (!dateStr) return '—';
    try {
      return new Date(dateStr).toLocaleDateString('en-GB', { day: '2-digit', month: 'short', year: 'numeric' });
    } catch { return dateStr; }
  };

  // Group by bank_code
  const grouped = useMemo(() => {
    const map = new Map<string, InProgressStatement[]>();
    for (const stmt of statements) {
      const key = stmt.bank_code || 'Unknown';
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(stmt);
    }
    return Array.from(map.entries()).sort(([a], [b]) => a.localeCompare(b));
  }, [statements]);

  if (loading) {
    return (
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
        <RefreshCw className="h-8 w-8 text-blue-400 mx-auto mb-2 animate-spin" />
        <p className="text-blue-700 text-sm font-medium">Loading in-progress statements...</p>
      </div>
    );
  }

  if (statements.length === 0) {
    return (
      <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
        <CheckCircle className="h-10 w-10 text-green-300 mx-auto mb-3" />
        <p className="text-gray-500 text-sm font-medium">No statements in progress awaiting reconciliation</p>
        <p className="text-gray-400 text-xs mt-1">Statements will appear here after import, until reconciliation is complete</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-sm text-gray-600">
          {statements.length} statement{statements.length !== 1 ? 's' : ''} imported but not yet reconciled
        </p>
        <button onClick={onRefresh} className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1">
          <RefreshCw className="h-3.5 w-3.5" /> Refresh
        </button>
      </div>

      {grouped.map(([bankCode, stmts]) => (
        <div key={bankCode} className="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="px-4 py-3 bg-gray-50 border-b border-gray-200 flex items-center gap-2">
            <Landmark className="h-4 w-4 text-blue-600" />
            <span className="text-sm font-medium text-gray-900">{bankCode}</span>
            <span className="px-2 py-0.5 text-xs font-medium bg-orange-100 text-orange-700 rounded-full">{stmts.length}</span>
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-xs text-gray-500 uppercase">
                <th className="px-4 py-2 text-left font-medium">Filename</th>
                <th className="px-4 py-2 text-left font-medium">Imported</th>
                <th className="px-4 py-2 text-right font-medium">Txns</th>
                <th className="px-4 py-2 text-right font-medium">Opening</th>
                <th className="px-4 py-2 text-right font-medium">Closing</th>
                <th className="px-4 py-2 text-right font-medium"></th>
              </tr>
            </thead>
            <tbody>
              {stmts.map(stmt => (
                <tr key={stmt.id} className="border-t border-gray-50 hover:bg-blue-50/30 transition-colors">
                  <td className="px-4 py-2">
                    <div className="flex items-center gap-1.5">
                      <FileText className="h-3.5 w-3.5 text-gray-400 flex-shrink-0" />
                      <span className="text-gray-800 font-medium truncate max-w-[250px]" title={stmt.filename}>{stmt.filename}</span>
                    </div>
                  </td>
                  <td className="px-4 py-2 text-xs text-gray-600">
                    <div className="flex items-center gap-1">
                      <Clock className="h-3 w-3 text-gray-400" />
                      {formatDate(stmt.import_date)}
                    </div>
                  </td>
                  <td className="px-4 py-2 text-right text-xs font-mono text-gray-700">
                    {stmt.transactions_imported}/{stmt.stored_transaction_count}
                    {stmt.transactions_imported < stmt.stored_transaction_count && (
                      <span className="ml-1 text-orange-600" title={`${stmt.stored_transaction_count - stmt.transactions_imported} not posted to Opera`}>
                        ({stmt.stored_transaction_count - stmt.transactions_imported} unposted)
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-right text-xs font-mono text-gray-700">{formatBal(stmt.opening_balance)}</td>
                  <td className="px-4 py-2 text-right text-xs font-mono text-gray-700">{formatBal(stmt.closing_balance)}</td>
                  <td className="px-4 py-2 text-right">
                    <div className="flex items-center gap-1.5 justify-end">
                      <button onClick={() => onReprocess(stmt)}
                        className="px-3 py-1 text-xs font-medium bg-gray-500 text-white rounded hover:bg-gray-600 flex items-center gap-1"
                        title="Clear import data and start over">
                        Reprocess
                      </button>
                      {stmt.transactions_imported < stmt.stored_transaction_count && (
                        <button onClick={() => onContinueImport(stmt)}
                          className="px-3 py-1 text-xs font-medium bg-orange-600 text-white rounded hover:bg-orange-700 flex items-center gap-1"
                          title={`${stmt.stored_transaction_count - stmt.transactions_imported} lines not yet posted to Opera`}>
                          Continue Import <ArrowRight className="h-3 w-3" />
                        </button>
                      )}
                      <button onClick={() => onResume(stmt)}
                        className="px-3 py-1 text-xs font-medium bg-green-600 text-white rounded hover:bg-green-700 flex items-center gap-1">
                        Reconcile <ArrowRight className="h-3 w-3" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  );
}

// ---- Manage Statements Tab ----

function ManageStatementsTab({
  nonCurrent,
  onRefresh,
}: {
  nonCurrent: NonCurrentStatements;
  onRefresh: () => void;
}) {
  const [selected, setSelected] = useState<Set<string>>(new Set()); // key: `${source}-${email_id}-${filename}`
  const [actionLoading, setActionLoading] = useState(false);
  const [actionResult, setActionResult] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);

  const stmtKey = (s: StatementEntry) => `${s.source}-${s.email_id || ''}-${s.filename}`;

  const toggleSelect = (s: StatementEntry) => {
    const key = stmtKey(s);
    setSelected(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const selectAll = (stmts: StatementEntry[]) => {
    setSelected(prev => {
      const next = new Set(prev);
      stmts.forEach(s => next.add(stmtKey(s)));
      return next;
    });
  };

  const allStatements = useMemo(() => [
    ...nonCurrent.already_processed,
    ...nonCurrent.old_statements,
    ...nonCurrent.not_classified,
  ], [nonCurrent]);

  const selectedStatements = useMemo(
    () => allStatements.filter(s => selected.has(stmtKey(s))),
    [allStatements, selected]
  );

  const handleAction = async (action: 'archive' | 'delete' | 'retain', stmts?: StatementEntry[]) => {
    const targets = stmts || selectedStatements;
    if (targets.length === 0) return;

    if (action === 'delete' && !confirmDelete) {
      setConfirmDelete(true);
      return;
    }
    setConfirmDelete(false);
    setActionLoading(true);
    setActionResult(null);
    try {
      const resp = await authFetch('/api/bank-import/manage-statements', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          action,
          statements: targets.map(s => ({
            source: s.source,
            email_id: s.email_id,
            attachment_id: s.attachment_id,
            filename: s.filename,
            full_path: s.full_path,
            matched_bank_code: s.matched_bank_code,
            category: s.category,
          })),
        }),
      });
      const data = await resp.json();
      if (data.success) {
        setActionResult(data.message);
        setSelected(new Set());
        // Refresh scan after action
        setTimeout(() => onRefresh(), 500);
      } else {
        setActionResult(`Error: ${data.error}`);
      }
    } catch (err: any) {
      setActionResult(`Error: ${err.message}`);
    } finally {
      setActionLoading(false);
    }
  };

  const categories: { key: keyof NonCurrentStatements; label: string; description: string; color: string; actions: ('archive' | 'delete' | 'retain')[] }[] = [
    { key: 'already_processed', label: 'Already Processed', description: 'Opening balance is behind reconciled — these have already been imported', color: 'gray', actions: ['archive', 'delete'] },
    { key: 'old_statements', label: 'Old Statements', description: 'Multiple statement periods behind — both opening and closing are below reconciled balance', color: 'gray', actions: ['archive', 'delete'] },
    { key: 'not_classified', label: 'Not Classified', description: 'Cannot be matched to any Opera bank account by sort code and account number', color: 'amber', actions: ['delete', 'retain'] },
    { key: 'advanced', label: 'Advanced', description: 'Opening balance is ahead of reconciled — there may be a missing intermediate statement', color: 'purple', actions: [] },
  ];

  return (
    <div className="space-y-4">
      {/* Bulk action bar */}
      {selected.size > 0 && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 flex items-center justify-between sticky top-0 z-10">
          <span className="text-sm font-medium text-blue-800">{selected.size} selected</span>
          <div className="flex items-center gap-2">
            <button onClick={() => handleAction('archive')} disabled={actionLoading}
              className="px-3 py-1.5 text-xs font-medium bg-gray-600 text-white rounded hover:bg-gray-700 disabled:opacity-50 flex items-center gap-1">
              <Archive className="h-3 w-3" /> Archive Selected
            </button>
            {confirmDelete ? (
              <div className="flex items-center gap-1">
                <span className="text-xs text-red-700">Confirm delete?</span>
                <button onClick={() => handleAction('delete')} className="px-2 py-1 text-xs bg-red-600 text-white rounded hover:bg-red-700">Yes</button>
                <button onClick={() => setConfirmDelete(false)} className="px-2 py-1 text-xs bg-gray-300 text-gray-700 rounded hover:bg-gray-400">No</button>
              </div>
            ) : (
              <button onClick={() => handleAction('delete')} disabled={actionLoading}
                className="px-3 py-1.5 text-xs font-medium bg-red-600 text-white rounded hover:bg-red-700 disabled:opacity-50 flex items-center gap-1">
                <Trash2 className="h-3 w-3" /> Delete Selected
              </button>
            )}
            <button onClick={() => setSelected(new Set())} className="px-3 py-1.5 text-xs text-gray-600 hover:text-gray-800">
              Clear
            </button>
          </div>
        </div>
      )}

      {/* Action result */}
      {actionResult && (
        <div className={`p-3 rounded-lg text-sm flex items-center justify-between ${
          actionResult.startsWith('Error') ? 'bg-red-50 border border-red-200 text-red-700' : 'bg-green-50 border border-green-200 text-green-700'
        }`}>
          <span>{actionResult}</span>
          <button onClick={() => setActionResult(null)} className="text-gray-400 hover:text-gray-600"><X className="h-4 w-4" /></button>
        </div>
      )}

      {/* Category sections */}
      {categories.map(cat => {
        const stmts = nonCurrent[cat.key];
        if (stmts.length === 0) return null;
        return (
          <CategorySection
            key={cat.key}
            label={cat.label}
            description={cat.description}
            color={cat.color}
            statements={stmts}
            actions={cat.actions}
            selected={selected}
            onToggleSelect={toggleSelect}
            onSelectAll={() => selectAll(stmts)}
            onAction={handleAction}
            actionLoading={actionLoading}
          />
        );
      })}

      {/* All empty */}
      {Object.values(nonCurrent).every(arr => arr.length === 0) && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-6 text-center">
          <CheckCircle className="h-8 w-8 text-green-400 mx-auto mb-2" />
          <p className="text-green-700 text-sm font-medium">No non-current statements to manage</p>
        </div>
      )}
    </div>
  );
}

// ---- Category Section ----

function CategorySection({
  label, description, color, statements, actions, selected, onToggleSelect, onSelectAll, onAction, actionLoading,
}: {
  label: string;
  description: string;
  color: string;
  statements: StatementEntry[];
  actions: ('archive' | 'delete' | 'retain')[];
  selected: Set<string>;
  onToggleSelect: (s: StatementEntry) => void;
  onSelectAll: () => void;
  onAction: (action: 'archive' | 'delete' | 'retain', stmts: StatementEntry[]) => void;
  actionLoading: boolean;
}) {
  const [expanded, setExpanded] = useState(true);
  const stmtKey = (s: StatementEntry) => `${s.source}-${s.email_id || ''}-${s.filename}`;

  const headerColors: Record<string, string> = {
    gray: 'bg-gray-50 border-gray-200',
    amber: 'bg-amber-50 border-amber-200',
    purple: 'bg-purple-50 border-purple-200',
  };

  const badgeColors: Record<string, string> = {
    gray: 'bg-gray-200 text-gray-700',
    amber: 'bg-amber-200 text-amber-800',
    purple: 'bg-purple-200 text-purple-800',
  };

  const formatBal = (val: number | undefined | null) => {
    if (val === null || val === undefined) return '—';
    return `£${val.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  return (
    <div className={`border rounded-lg overflow-hidden ${headerColors[color] || 'bg-gray-50 border-gray-200'}`}>
      <button onClick={() => setExpanded(!expanded)}
        className="w-full px-4 py-3 flex items-center justify-between hover:bg-white/30 transition-colors">
        <div className="flex items-center gap-2">
          {expanded ? <ChevronDown className="h-4 w-4 text-gray-400" /> : <ChevronRight className="h-4 w-4 text-gray-400" />}
          <span className="text-sm font-medium text-gray-900">{label}</span>
          <span className={`px-2 py-0.5 text-xs font-medium rounded-full ${badgeColors[color] || 'bg-gray-200 text-gray-700'}`}>
            {statements.length}
          </span>
        </div>
        {actions.length > 0 && expanded && (
          <div className="flex items-center gap-1" onClick={e => e.stopPropagation()}>
            <button onClick={onSelectAll} className="text-xs text-blue-600 hover:text-blue-800 mr-2">Select all</button>
            {actions.includes('archive') && (
              <button onClick={() => onAction('archive', statements)} disabled={actionLoading}
                className="px-2 py-1 text-xs bg-gray-500 text-white rounded hover:bg-gray-600 disabled:opacity-50">
                Archive All
              </button>
            )}
          </div>
        )}
      </button>

      {expanded && (
        <div className="bg-white">
          <p className="px-4 py-2 text-xs text-gray-500 border-t border-gray-100">{description}</p>
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-xs text-gray-500 uppercase border-t border-gray-100">
                {actions.length > 0 && <th className="px-4 py-2 w-8"></th>}
                <th className="px-4 py-2 text-left font-medium">Filename</th>
                <th className="px-4 py-2 text-left font-medium">Source</th>
                <th className="px-4 py-2 text-left font-medium">Bank</th>
                <th className="px-4 py-2 text-right font-medium">Opening</th>
                <th className="px-4 py-2 text-right font-medium">Closing</th>
                <th className="px-4 py-2 text-right font-medium">Gap</th>
                {actions.length > 0 && <th className="px-4 py-2 text-right font-medium">Actions</th>}
              </tr>
            </thead>
            <tbody>
              {statements.map((stmt, i) => (
                <tr key={i} className="border-t border-gray-50 hover:bg-gray-50/50">
                  {actions.length > 0 && (
                    <td className="px-4 py-2">
                      <input type="checkbox" checked={selected.has(stmtKey(stmt))}
                        onChange={() => onToggleSelect(stmt)}
                        className="rounded border-gray-300" />
                    </td>
                  )}
                  <td className="px-4 py-2">
                    <div className="flex items-center gap-1.5">
                      <FileText className="h-3.5 w-3.5 text-gray-400 flex-shrink-0" />
                      <span className="text-gray-800 font-medium truncate max-w-[220px]" title={stmt.filename}>{stmt.filename}</span>
                    </div>
                  </td>
                  <td className="px-4 py-2">
                    {stmt.source === 'email' ? (
                      <span className="inline-flex items-center gap-1 text-xs text-purple-600"><Mail className="h-3 w-3" /> Email</span>
                    ) : (
                      <span className="inline-flex items-center gap-1 text-xs text-green-600"><FolderOpen className="h-3 w-3" /> File</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-xs text-gray-600">
                    {stmt.matched_bank_code ? (
                      <div>
                        <span className="font-medium">{stmt.matched_bank_code}</span>
                        <span className="text-gray-400 mx-1">—</span>
                        <span>{stmt.matched_bank_description}</span>
                        {(stmt.matched_sort_code || stmt.matched_account_number) && (
                          <div className="text-[10px] text-gray-400 mt-0.5 font-mono">
                            {stmt.matched_sort_code && `${stmt.matched_sort_code.replace(/(\d{2})(\d{2})(\d{2})/, '$1-$2-$3')}`}
                            {stmt.matched_sort_code && stmt.matched_account_number && ' / '}
                            {stmt.matched_account_number}
                          </div>
                        )}
                      </div>
                    ) : (
                      <span className="text-gray-400">—</span>
                    )}
                  </td>
                  <td className="px-4 py-2 text-right text-xs font-mono text-gray-700">{formatBal(stmt.opening_balance)}</td>
                  <td className="px-4 py-2 text-right text-xs font-mono text-gray-700">{formatBal(stmt.closing_balance)}</td>
                  <td className="px-4 py-2 text-right text-xs font-mono text-amber-600">
                    {stmt.balance_gap != null ? `£${stmt.balance_gap.toLocaleString('en-GB', { minimumFractionDigits: 2 })}` : '—'}
                  </td>
                  {actions.length > 0 && (
                    <td className="px-4 py-2 text-right">
                      <div className="flex items-center gap-1 justify-end">
                        {actions.includes('archive') && (
                          <button onClick={() => onAction('archive', [stmt])} disabled={actionLoading}
                            title="Archive" className="p-1 text-gray-400 hover:text-gray-600 disabled:opacity-50">
                            <Archive className="h-3.5 w-3.5" />
                          </button>
                        )}
                        {actions.includes('delete') && (
                          <button onClick={() => onAction('delete', [stmt])} disabled={actionLoading}
                            title="Delete" className="p-1 text-gray-400 hover:text-red-600 disabled:opacity-50">
                            <Trash2 className="h-3.5 w-3.5" />
                          </button>
                        )}
                        {actions.includes('retain') && (
                          <button onClick={() => onAction('retain', [stmt])} disabled={actionLoading}
                            title="Retain (keep but hide from scan)" className="p-1 text-gray-400 hover:text-blue-600 disabled:opacity-50">
                            <Eye className="h-3.5 w-3.5" />
                          </button>
                        )}
                      </div>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ---- Bank Card ----

function BankCard({ bank, expanded, onToggle, onProcess, onReconcile }: {
  bank: BankGroup; expanded: boolean; onToggle: () => void; onProcess: (stmt: StatementEntry) => void; onReconcile: (stmt: StatementEntry) => void;
}) {
  const readyCount = bank.statements.filter(s => s.status === 'ready').length;

  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      <button onClick={onToggle}
        className="w-full px-4 py-3 flex items-center justify-between hover:bg-gray-50 transition-colors">
        <div className="flex items-center gap-3">
          {expanded ? <ChevronDown className="h-4 w-4 text-gray-400" /> : <ChevronRight className="h-4 w-4 text-gray-400" />}
          <Landmark className="h-5 w-5 text-blue-600" />
          <div className="text-left">
            <div className="text-sm font-medium text-gray-900">
              {bank.description}
              <span className="text-gray-400 ml-2 font-normal">{bank.bank_code}</span>
            </div>
            <div className="text-xs text-gray-500">
              {bank.sort_code} / {bank.account_number}
              {bank.reconciled_balance !== null && (
                <span className="ml-2">
                  Reconciled: <span className="font-medium text-gray-700">£{bank.reconciled_balance.toLocaleString('en-GB', { minimumFractionDigits: 2 })}</span>
                </span>
              )}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {readyCount > 0 && <span className="px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-700 rounded-full">{readyCount} ready</span>}
          <span className="text-xs text-gray-400">{bank.statement_count} statement{bank.statement_count !== 1 ? 's' : ''}</span>
        </div>
      </button>

      {expanded && (
        <div className="border-t border-gray-100">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-xs text-gray-500 uppercase">
                <th className="px-4 py-2 text-left font-medium">#</th>
                <th className="px-4 py-2 text-left font-medium">Filename</th>
                <th className="px-4 py-2 text-left font-medium">Source</th>
                <th className="px-4 py-2 text-left font-medium">Period</th>
                <th className="px-4 py-2 text-right font-medium">Opening</th>
                <th className="px-4 py-2 text-right font-medium">Closing</th>
                <th className="px-4 py-2 text-center font-medium">Status</th>
                <th className="px-4 py-2 text-right font-medium"></th>
              </tr>
            </thead>
            <tbody>
              {bank.statements.map((stmt, idx) => (
                <StatementRow key={idx} stmt={stmt} onProcess={() => onProcess(stmt)} onReconcile={stmt.status === 'imported' ? () => onReconcile(stmt) : undefined} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ---- Statement Row ----

function StatementRow({ stmt, onProcess, onReconcile }: { stmt: StatementEntry; onProcess: () => void; onReconcile?: () => void }) {
  const statusBadge = useMemo(() => {
    switch (stmt.status) {
      case 'ready':
        return <span className="px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-700 rounded-full">Ready</span>;
      case 'imported':
        return <span className="px-2 py-0.5 text-xs font-medium bg-orange-100 text-orange-700 rounded-full" title="Imported but not yet reconciled">Awaiting Reconcile</span>;
      case 'uncached':
        return <span className="px-2 py-0.5 text-xs font-medium bg-gray-100 text-gray-500 rounded-full">Uncached</span>;
      default:
        return <span className="px-2 py-0.5 text-xs font-medium bg-gray-100 text-gray-500 rounded-full">Pending</span>;
    }
  }, [stmt.status, stmt.validation_note]);

  const formatBal = (val: number | undefined | null) => {
    if (val === null || val === undefined) return '—';
    return `£${val.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const formatPeriod = () => {
    if (stmt.period_start && stmt.period_end) return `${stmt.period_start} → ${stmt.period_end}`;
    if (stmt.period_end) return stmt.period_end;
    if (stmt.statement_date) return stmt.statement_date;
    return '—';
  };

  return (
    <tr className="border-t border-gray-50 hover:bg-blue-50/30 transition-colors">
      <td className="px-4 py-2 text-gray-400 text-xs">{stmt.import_sequence}</td>
      <td className="px-4 py-2">
        <div className="flex items-center gap-1.5">
          <FileText className="h-3.5 w-3.5 text-gray-400 flex-shrink-0" />
          <span className="text-gray-800 font-medium truncate max-w-[250px]" title={stmt.filename}>{stmt.filename}</span>
        </div>
      </td>
      <td className="px-4 py-2">
        {stmt.source === 'email' ? (
          <span className="inline-flex items-center gap-1 text-xs text-purple-600"><Mail className="h-3 w-3" /> Email</span>
        ) : (
          <span className="inline-flex items-center gap-1 text-xs text-green-600"><FolderOpen className="h-3 w-3" /> File</span>
        )}
      </td>
      <td className="px-4 py-2 text-xs text-gray-600">{formatPeriod()}</td>
      <td className="px-4 py-2 text-right text-xs font-mono text-gray-700">{formatBal(stmt.opening_balance)}</td>
      <td className="px-4 py-2 text-right text-xs font-mono text-gray-700">{formatBal(stmt.closing_balance)}</td>
      <td className="px-4 py-2 text-center">{statusBadge}</td>
      <td className="px-4 py-2 text-right">
        {onReconcile ? (
          <button onClick={onReconcile}
            className="px-3 py-1 text-xs font-medium bg-green-600 text-white rounded hover:bg-green-700 flex items-center gap-1 ml-auto">
            Reconcile <ArrowRight className="h-3 w-3" />
          </button>
        ) : (
          <button onClick={onProcess} disabled={stmt.status === 'already_processed'}
            className="px-3 py-1 text-xs font-medium bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-1 ml-auto">
            Process <ArrowRight className="h-3 w-3" />
          </button>
        )}
      </td>
    </tr>
  );
}
