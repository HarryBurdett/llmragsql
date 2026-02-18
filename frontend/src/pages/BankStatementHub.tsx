import { useState, useCallback, useMemo } from 'react';
import { Landmark, RefreshCw, FileText, ArrowRight, CheckCircle, AlertTriangle, Search, ChevronDown, ChevronRight, Mail, FolderOpen, X } from 'lucide-react';
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
  status: 'ready' | 'sequence_gap' | 'uncached' | 'pending' | 'already_processed';
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

interface ScanResult {
  success: boolean;
  banks: Record<string, BankGroup>;
  unidentified: StatementEntry[];
  total_statements: number;
  total_banks_with_statements: number;
  total_banks_loaded: number;
  total_emails_scanned: number;
  total_pdfs_found: number;
  days_searched: number;
  message: string;
  error?: string;
}

type TabType = 'pending' | 'process' | 'reconcile';

// Handoff data from import to reconcile
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

  // Selected statement for processing
  const [selectedStatement, setSelectedStatement] = useState<{
    bankCode: string;
    bankDescription: string;
    statement: StatementEntry;
  } | null>(null);

  // Reconcile data from import complete
  const [reconcileData, setReconcileData] = useState<ReconcileHandoff | null>(null);

  // Expanded bank cards
  const [expandedBanks, setExpandedBanks] = useState<Set<string>>(new Set());

  // ---- Scan handler ----
  const handleScan = useCallback(async () => {
    setScanning(true);
    setScanError(null);
    try {
      const resp = await authFetch(`/api/bank-import/scan-all-banks?days_back=${daysBack}&validate_balances=true`);
      const data: ScanResult = await resp.json();
      if (data.success) {
        setScanResult(data);
        setLastScanTime(new Date().toLocaleTimeString());
        // Auto-expand banks that have statements
        setExpandedBanks(new Set(Object.keys(data.banks)));
      } else {
        setScanError(data.error || 'Scan failed');
      }
    } catch (err: any) {
      setScanError(err.message || 'Network error');
    } finally {
      setScanning(false);
    }
  }, [daysBack]);

  // ---- Process statement handler ----
  const handleProcess = useCallback((bankCode: string, bankDescription: string, stmt: StatementEntry) => {
    setSelectedStatement({ bankCode, bankDescription, statement: stmt });
    setReconcileData(null);
    setActiveTab('process');
  }, []);

  // ---- Import complete handler ----
  const handleImportComplete = useCallback((data: ReconcileHandoff) => {
    setReconcileData(data);
    setActiveTab('reconcile');
  }, []);

  // ---- Reconcile complete handler ----
  const handleReconcileComplete = useCallback(() => {
    setSelectedStatement(null);
    setReconcileData(null);
    setActiveTab('pending');
    // Auto-refresh scan
    handleScan();
  }, [handleScan]);

  // ---- Back to pending ----
  const handleBackToPending = useCallback(() => {
    setActiveTab('pending');
  }, []);

  // Toggle bank card
  const toggleBank = useCallback((code: string) => {
    setExpandedBanks(prev => {
      const next = new Set(prev);
      if (next.has(code)) next.delete(code);
      else next.add(code);
      return next;
    });
  }, []);

  // ---- Computed values ----
  const bankList = useMemo(() => {
    if (!scanResult?.banks) return [];
    return Object.values(scanResult.banks).sort((a, b) => a.bank_code.localeCompare(b.bank_code));
  }, [scanResult]);

  // ---- Tab definitions ----
  const tabs: { key: TabType; label: string; disabled: boolean }[] = [
    { key: 'pending', label: 'Pending Statements', disabled: false },
    { key: 'process', label: 'Process & Import', disabled: !selectedStatement },
    { key: 'reconcile', label: 'Reconcile', disabled: !reconcileData },
  ];

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Landmark className="h-6 w-6 text-blue-600" />
          <h1 className="text-xl font-semibold text-gray-900">Bank Statements</h1>
        </div>
      </div>

      {/* Tabs */}
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
            {tab.key === 'pending' && scanResult && (
              <span className="ml-1.5 text-xs bg-blue-100 text-blue-700 rounded-full px-1.5 py-0.5">
                {scanResult.total_statements}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Tab content */}
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
          onScan={handleScan}
          onProcess={handleProcess}
        />
      )}

      {activeTab === 'process' && selectedStatement && (
        <div>
          <div className="mb-3 flex items-center gap-2">
            <button
              onClick={handleBackToPending}
              className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1"
            >
              <ArrowRight className="h-3.5 w-3.5 rotate-180" />
              Back to Pending
            </button>
            <span className="text-gray-300">|</span>
            <span className="text-sm text-gray-600">
              Processing: <strong>{selectedStatement.statement.filename}</strong> for {selectedStatement.bankDescription}
            </span>
          </div>
          <Imports
            bankRecOnly
            initialStatement={{
              bankCode: selectedStatement.bankCode,
              emailId: selectedStatement.statement.email_id,
              attachmentId: selectedStatement.statement.attachment_id,
              filename: selectedStatement.statement.filename,
              source: selectedStatement.statement.source,
              fullPath: selectedStatement.statement.full_path,
            }}
            onImportComplete={handleImportComplete}
          />
        </div>
      )}

      {activeTab === 'reconcile' && reconcileData && (
        <div>
          <div className="mb-3 flex items-center gap-2">
            <button
              onClick={handleBackToPending}
              className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1"
            >
              <ArrowRight className="h-3.5 w-3.5 rotate-180" />
              Back to Pending
            </button>
            <span className="text-gray-300">|</span>
            <span className="text-sm text-gray-600">
              Reconciling: <strong>{reconcileData.filename || 'Statement'}</strong>
            </span>
          </div>
          <BankStatementReconcile
            initialReconcileData={reconcileData}
            onReconcileComplete={handleReconcileComplete}
          />
        </div>
      )}
    </div>
  );
}

// ---- Pending Statements Tab ----

function PendingStatementsTab({
  scanResult,
  bankList,
  scanning,
  scanError,
  lastScanTime,
  daysBack,
  setDaysBack,
  expandedBanks,
  toggleBank,
  onScan,
  onProcess,
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
  onScan: () => void;
  onProcess: (bankCode: string, bankDescription: string, stmt: StatementEntry) => void;
}) {
  return (
    <div className="space-y-4">
      {/* Scan controls */}
      <div className="bg-white border border-gray-200 rounded-lg p-4">
        <div className="flex items-center justify-between flex-wrap gap-3">
          <div className="flex items-center gap-3">
            <button
              onClick={onScan}
              disabled={scanning}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2 text-sm font-medium"
            >
              {scanning ? (
                <RefreshCw className="h-4 w-4 animate-spin" />
              ) : (
                <Search className="h-4 w-4" />
              )}
              {scanning ? 'Scanning...' : 'Scan All Banks'}
            </button>

            <div className="flex items-center gap-1.5 text-sm text-gray-600">
              <span>Last</span>
              <select
                value={daysBack}
                onChange={e => setDaysBack(Number(e.target.value))}
                className="border border-gray-300 rounded px-2 py-1 text-sm"
              >
                <option value={7}>7 days</option>
                <option value={14}>14 days</option>
                <option value={30}>30 days</option>
                <option value={60}>60 days</option>
                <option value={90}>90 days</option>
              </select>
            </div>
          </div>

          {lastScanTime && (
            <span className="text-xs text-gray-400">
              Last scan: {lastScanTime}
            </span>
          )}
        </div>

        {/* Scan summary */}
        {scanResult && !scanning && (
          <div className="mt-3 text-sm text-gray-600">
            {scanResult.message}
          </div>
        )}
      </div>

      {/* Error */}
      {scanError && (
        <div className="p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
          <span className="text-red-500">⚠</span>
          <div className="flex-1">
            <p className="text-sm text-red-800 font-medium">Scan Error</p>
            <p className="text-sm text-red-700">{scanError}</p>
          </div>
          <button onClick={() => {}} className="text-red-400 hover:text-red-600">
            <X className="h-4 w-4" />
          </button>
        </div>
      )}

      {/* No scan yet */}
      {!scanResult && !scanning && !scanError && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-8 text-center">
          <Landmark className="h-10 w-10 text-gray-300 mx-auto mb-3" />
          <p className="text-gray-500 text-sm">Click "Scan All Banks" to find pending statements across all bank accounts</p>
        </div>
      )}

      {/* Scanning spinner */}
      {scanning && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-center">
          <RefreshCw className="h-8 w-8 text-blue-400 mx-auto mb-2 animate-spin" />
          <p className="text-blue-700 text-sm font-medium">Scanning emails and files for bank statements...</p>
          <p className="text-blue-500 text-xs mt-1">Checking PDF cache for account matching</p>
        </div>
      )}

      {/* Bank cards */}
      {scanResult && !scanning && bankList.length > 0 && (
        <div className="space-y-3">
          {bankList.map(bank => (
            <BankCard
              key={bank.bank_code}
              bank={bank}
              expanded={expandedBanks.has(bank.bank_code)}
              onToggle={() => toggleBank(bank.bank_code)}
              onProcess={(stmt) => onProcess(bank.bank_code, bank.description, stmt)}
            />
          ))}
        </div>
      )}

      {/* No statements found */}
      {scanResult && !scanning && bankList.length === 0 && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-6 text-center">
          <CheckCircle className="h-8 w-8 text-green-400 mx-auto mb-2" />
          <p className="text-green-700 text-sm font-medium">All bank statements are up to date</p>
          <p className="text-green-600 text-xs mt-1">No pending statements found across {scanResult.total_banks_loaded} bank accounts</p>
        </div>
      )}

      {/* Unidentified statements */}
      {scanResult && scanResult.unidentified.length > 0 && !scanning && (
        <div className="bg-amber-50 border border-amber-200 rounded-lg p-4">
          <div className="flex items-center gap-2 mb-2">
            <AlertTriangle className="h-4 w-4 text-amber-600" />
            <h3 className="text-sm font-medium text-amber-800">
              Unidentified Statements ({scanResult.unidentified.length})
            </h3>
          </div>
          <p className="text-xs text-amber-600 mb-2">
            These statements couldn't be matched to an Opera bank account. They may need to be processed via the extraction cache first.
          </p>
          <div className="space-y-1">
            {scanResult.unidentified.map((stmt, i) => (
              <div key={i} className="text-xs text-amber-700 flex items-center gap-2 py-1">
                {stmt.source === 'email' ? <Mail className="h-3 w-3" /> : <FolderOpen className="h-3 w-3" />}
                <span className="font-medium">{stmt.filename}</span>
                {stmt.status === 'uncached' && (
                  <span className="text-amber-500 italic">— not in extraction cache</span>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ---- Bank Card ----

function BankCard({
  bank,
  expanded,
  onToggle,
  onProcess,
}: {
  bank: BankGroup;
  expanded: boolean;
  onToggle: () => void;
  onProcess: (stmt: StatementEntry) => void;
}) {
  const readyCount = bank.statements.filter(s => s.status === 'ready').length;
  const gapCount = bank.statements.filter(s => s.status === 'sequence_gap').length;

  return (
    <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
      {/* Bank header */}
      <button
        onClick={onToggle}
        className="w-full px-4 py-3 flex items-center justify-between hover:bg-gray-50 transition-colors"
      >
        <div className="flex items-center gap-3">
          {expanded ? (
            <ChevronDown className="h-4 w-4 text-gray-400" />
          ) : (
            <ChevronRight className="h-4 w-4 text-gray-400" />
          )}
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
          {readyCount > 0 && (
            <span className="px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-700 rounded-full">
              {readyCount} ready
            </span>
          )}
          {gapCount > 0 && (
            <span className="px-2 py-0.5 text-xs font-medium bg-amber-100 text-amber-700 rounded-full">
              {gapCount} gap
            </span>
          )}
          <span className="text-xs text-gray-400">
            {bank.statement_count} statement{bank.statement_count !== 1 ? 's' : ''}
          </span>
        </div>
      </button>

      {/* Statement rows */}
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
                <StatementRow
                  key={idx}
                  stmt={stmt}
                  onProcess={() => onProcess(stmt)}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ---- Statement Row ----

function StatementRow({
  stmt,
  onProcess,
}: {
  stmt: StatementEntry;
  onProcess: () => void;
}) {
  const statusBadge = useMemo(() => {
    switch (stmt.status) {
      case 'ready':
        return <span className="px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-700 rounded-full">Ready</span>;
      case 'sequence_gap':
        return (
          <span className="px-2 py-0.5 text-xs font-medium bg-amber-100 text-amber-700 rounded-full" title={stmt.validation_note}>
            Gap
          </span>
        );
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
    if (stmt.period_start && stmt.period_end) {
      return `${stmt.period_start} → ${stmt.period_end}`;
    }
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
          <span className="text-gray-800 font-medium truncate max-w-[250px]" title={stmt.filename}>
            {stmt.filename}
          </span>
        </div>
      </td>
      <td className="px-4 py-2">
        {stmt.source === 'email' ? (
          <span className="inline-flex items-center gap-1 text-xs text-purple-600">
            <Mail className="h-3 w-3" /> Email
          </span>
        ) : (
          <span className="inline-flex items-center gap-1 text-xs text-green-600">
            <FolderOpen className="h-3 w-3" /> File
          </span>
        )}
      </td>
      <td className="px-4 py-2 text-xs text-gray-600">{formatPeriod()}</td>
      <td className="px-4 py-2 text-right text-xs font-mono text-gray-700">{formatBal(stmt.opening_balance)}</td>
      <td className="px-4 py-2 text-right text-xs font-mono text-gray-700">{formatBal(stmt.closing_balance)}</td>
      <td className="px-4 py-2 text-center">{statusBadge}</td>
      <td className="px-4 py-2 text-right">
        <button
          onClick={onProcess}
          disabled={stmt.status === 'already_processed'}
          className="px-3 py-1 text-xs font-medium bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed flex items-center gap-1 ml-auto"
        >
          Process <ArrowRight className="h-3 w-3" />
        </button>
      </td>
    </tr>
  );
}
