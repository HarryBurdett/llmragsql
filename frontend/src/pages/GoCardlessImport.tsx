import { useState } from 'react';
import { CreditCard, Upload, CheckCircle, AlertCircle, Search, ArrowRight } from 'lucide-react';

interface Payment {
  customer_name: string;
  description: string;
  amount: number;
  invoice_refs: string[];
  matched_account?: string;
  matched_name?: string;
  match_score?: number;
  match_status?: 'matched' | 'review' | 'unmatched';
}

interface ParseResult {
  success: boolean;
  error?: string;
  payment_count?: number;
  gross_amount?: number;
  gocardless_fees?: number;
  vat_on_fees?: number;
  net_amount?: number;
  bank_reference?: string;
  payments?: Payment[];
}

interface MatchResult {
  success: boolean;
  error?: string;
  payments?: Payment[];
  matched_count?: number;
  review_count?: number;
  unmatched_count?: number;
}

interface Customer {
  account: string;
  name: string;
}

export function GoCardlessImport() {
  const [emailContent, setEmailContent] = useState('');
  const [parseResult, setParseResult] = useState<ParseResult | null>(null);
  const [matchedPayments, setMatchedPayments] = useState<Payment[]>([]);
  const [customers, setCustomers] = useState<Customer[]>([]);
  const [isParsing, setIsParsing] = useState(false);
  const [isImporting, setIsImporting] = useState(false);
  const [importResult, setImportResult] = useState<{ success: boolean; message: string } | null>(null);
  const [bankCode, setBankCode] = useState('BC010');
  const [postDate, setPostDate] = useState(new Date().toISOString().split('T')[0]);
  const [completeBatch, setCompleteBatch] = useState(false);

  // Parse the pasted email content
  const handleParse = async () => {
    if (!emailContent.trim()) {
      setParseResult({ success: false, error: 'Please paste GoCardless email content' });
      return;
    }

    setIsParsing(true);
    setParseResult(null);
    setMatchedPayments([]);

    try {
      const response = await fetch('/api/gocardless/parse', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(emailContent)
      });
      const data = await response.json();
      setParseResult(data);

      if (data.success && data.payments) {
        // Automatically match customers
        await matchCustomers(data.payments);
      }
    } catch (error) {
      setParseResult({ success: false, error: `Failed to parse: ${error}` });
    } finally {
      setIsParsing(false);
    }
  };

  // Match parsed payments to Opera customers
  const matchCustomers = async (payments: Payment[]) => {
    try {
      // Fetch customers list if not already loaded
      if (customers.length === 0) {
        const custResponse = await fetch('/api/bank-import/accounts/customers');
        const custData = await custResponse.json();
        if (custData.success && custData.accounts) {
          setCustomers(custData.accounts);
        }
      }

      // Match customers
      const response = await fetch('/api/gocardless/match-customers', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payments)
      });
      const data: MatchResult = await response.json();

      if (data.success && data.payments) {
        setMatchedPayments(data.payments);
      }
    } catch (error) {
      console.error('Customer matching failed:', error);
      // Fall back to unmatched payments
      setMatchedPayments(payments.map(p => ({ ...p, match_status: 'unmatched' as const })));
    }
  };

  // Update a payment's matched account
  const updatePaymentAccount = (index: number, account: string, name: string) => {
    setMatchedPayments(prev => {
      const updated = [...prev];
      updated[index] = {
        ...updated[index],
        matched_account: account,
        matched_name: name,
        match_status: account ? 'matched' : 'unmatched'
      };
      return updated;
    });
  };

  // Import the batch
  const handleImport = async () => {
    const paymentsToImport = matchedPayments.filter(p => p.matched_account);

    if (paymentsToImport.length === 0) {
      setImportResult({ success: false, message: 'No payments have customer accounts assigned' });
      return;
    }

    setIsImporting(true);
    setImportResult(null);

    try {
      const response = await fetch(
        `/api/gocardless/import?bank_code=${bankCode}&post_date=${postDate}&reference=GoCardless&complete_batch=${completeBatch}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(paymentsToImport.map(p => ({
            customer_account: p.matched_account,
            amount: p.amount,
            description: p.description || p.customer_name
          })))
        }
      );
      const data = await response.json();

      if (data.success) {
        setImportResult({
          success: true,
          message: `Successfully imported ${data.payments_imported} payments${completeBatch ? ' (completed)' : ' (pending review in Opera)'}`
        });
        // Clear form on success
        setEmailContent('');
        setParseResult(null);
        setMatchedPayments([]);
      } else {
        setImportResult({ success: false, message: data.error || 'Import failed' });
      }
    } catch (error) {
      setImportResult({ success: false, message: `Import failed: ${error}` });
    } finally {
      setIsImporting(false);
    }
  };

  const totalAmount = matchedPayments.reduce((sum, p) => sum + p.amount, 0);
  const matchedCount = matchedPayments.filter(p => p.matched_account).length;
  const unmatchedCount = matchedPayments.filter(p => !p.matched_account).length;

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-3">
        <CreditCard className="h-8 w-8 text-blue-600" />
        <h1 className="text-2xl font-bold text-gray-900">GoCardless Import</h1>
      </div>

      {/* Step 1: Paste Email Content */}
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
          <span className="bg-blue-100 text-blue-700 rounded-full w-6 h-6 flex items-center justify-center text-sm">1</span>
          Paste GoCardless Email
        </h2>
        <textarea
          className="w-full h-48 p-3 border border-gray-300 rounded-lg font-mono text-sm resize-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          placeholder="Paste the GoCardless payment notification email content here...

Example:
Customer                Description              Amount
Deep Blue Ltd           Intsys INV26362         7,380.00 GBP
Medimpex UK Ltd         Intsys INV26365         1,530.00 GBP
..."
          value={emailContent}
          onChange={(e) => setEmailContent(e.target.value)}
        />
        <div className="mt-4 flex justify-end">
          <button
            onClick={handleParse}
            disabled={isParsing || !emailContent.trim()}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
          >
            {isParsing ? (
              <>
                <div className="animate-spin h-4 w-4 border-2 border-white border-t-transparent rounded-full" />
                Parsing...
              </>
            ) : (
              <>
                <Search className="h-4 w-4" />
                Parse & Match
              </>
            )}
          </button>
        </div>
      </div>

      {/* Parse Error */}
      {parseResult && !parseResult.success && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 flex items-start gap-3">
          <AlertCircle className="h-5 w-5 text-red-500 flex-shrink-0 mt-0.5" />
          <div>
            <p className="font-medium text-red-800">Failed to parse</p>
            <p className="text-sm text-red-600">{parseResult.error}</p>
          </div>
        </div>
      )}

      {/* Step 2: Review & Match */}
      {matchedPayments.length > 0 && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <span className="bg-blue-100 text-blue-700 rounded-full w-6 h-6 flex items-center justify-center text-sm">2</span>
            Review & Match Customers
          </h2>

          {/* Summary */}
          <div className="grid grid-cols-4 gap-4 mb-6">
            <div className="bg-gray-50 rounded-lg p-3 text-center">
              <p className="text-2xl font-bold text-gray-900">{matchedPayments.length}</p>
              <p className="text-sm text-gray-500">Payments</p>
            </div>
            <div className="bg-green-50 rounded-lg p-3 text-center">
              <p className="text-2xl font-bold text-green-600">{matchedCount}</p>
              <p className="text-sm text-gray-500">Matched</p>
            </div>
            <div className="bg-yellow-50 rounded-lg p-3 text-center">
              <p className="text-2xl font-bold text-yellow-600">{unmatchedCount}</p>
              <p className="text-sm text-gray-500">Need Review</p>
            </div>
            <div className="bg-blue-50 rounded-lg p-3 text-center">
              <p className="text-2xl font-bold text-blue-600">
                £{totalAmount.toLocaleString('en-GB', { minimumFractionDigits: 2 })}
              </p>
              <p className="text-sm text-gray-500">Total</p>
            </div>
          </div>

          {/* GoCardless Summary */}
          {parseResult && parseResult.gocardless_fees !== undefined && parseResult.gocardless_fees !== 0 && (
            <div className="mb-4 p-3 bg-gray-50 rounded-lg text-sm">
              <div className="flex justify-between">
                <span>Gross Amount:</span>
                <span className="font-medium">£{parseResult.gross_amount?.toLocaleString('en-GB', { minimumFractionDigits: 2 })}</span>
              </div>
              <div className="flex justify-between text-red-600">
                <span>GoCardless Fees:</span>
                <span>-£{Math.abs(parseResult.gocardless_fees || 0).toLocaleString('en-GB', { minimumFractionDigits: 2 })}</span>
              </div>
              {parseResult.vat_on_fees !== undefined && parseResult.vat_on_fees !== 0 && (
                <div className="flex justify-between text-red-600">
                  <span>VAT on Fees:</span>
                  <span>-£{Math.abs(parseResult.vat_on_fees).toLocaleString('en-GB', { minimumFractionDigits: 2 })}</span>
                </div>
              )}
              <div className="flex justify-between font-medium border-t pt-1 mt-1">
                <span>Net Amount:</span>
                <span>£{parseResult.net_amount?.toLocaleString('en-GB', { minimumFractionDigits: 2 })}</span>
              </div>
              <p className="text-xs text-gray-500 mt-2">
                Note: Fees and VAT should be posted separately as a nominal entry in Opera cashbook.
              </p>
            </div>
          )}

          {/* Payments Table */}
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50">
                  <th className="text-left p-3 font-medium text-gray-700">Customer (from email)</th>
                  <th className="text-left p-3 font-medium text-gray-700">Description</th>
                  <th className="text-right p-3 font-medium text-gray-700">Amount</th>
                  <th className="text-left p-3 font-medium text-gray-700">Opera Account</th>
                  <th className="text-center p-3 font-medium text-gray-700">Status</th>
                </tr>
              </thead>
              <tbody>
                {matchedPayments.map((payment, idx) => (
                  <tr key={idx} className={`border-b ${!payment.matched_account ? 'bg-yellow-50' : ''}`}>
                    <td className="p-3">
                      <div className="font-medium">{payment.customer_name}</div>
                      {payment.invoice_refs && payment.invoice_refs.length > 0 && (
                        <div className="text-xs text-gray-500">
                          Refs: {payment.invoice_refs.join(', ')}
                        </div>
                      )}
                    </td>
                    <td className="p-3 text-gray-600">{payment.description}</td>
                    <td className="p-3 text-right font-medium">
                      £{payment.amount.toLocaleString('en-GB', { minimumFractionDigits: 2 })}
                    </td>
                    <td className="p-3">
                      <select
                        className="w-full p-2 border border-gray-300 rounded text-sm"
                        value={payment.matched_account || ''}
                        onChange={(e) => {
                          const account = e.target.value;
                          const customer = customers.find(c => c.account === account);
                          updatePaymentAccount(idx, account, customer?.name || '');
                        }}
                      >
                        <option value="">-- Select Customer --</option>
                        {customers.map(c => (
                          <option key={c.account} value={c.account}>
                            {c.account} - {c.name}
                          </option>
                        ))}
                      </select>
                      {payment.matched_name && (
                        <div className="text-xs text-gray-500 mt-1">{payment.matched_name}</div>
                      )}
                    </td>
                    <td className="p-3 text-center">
                      {payment.matched_account ? (
                        <span className="inline-flex items-center gap-1 px-2 py-1 bg-green-100 text-green-700 rounded-full text-xs">
                          <CheckCircle className="h-3 w-3" />
                          Matched
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 px-2 py-1 bg-yellow-100 text-yellow-700 rounded-full text-xs">
                          <AlertCircle className="h-3 w-3" />
                          Select
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

      {/* Step 3: Import */}
      {matchedPayments.length > 0 && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
            <span className="bg-blue-100 text-blue-700 rounded-full w-6 h-6 flex items-center justify-center text-sm">3</span>
            Import to Opera
          </h2>

          <div className="grid grid-cols-3 gap-4 mb-6">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Bank Account</label>
              <select
                className="w-full p-2 border border-gray-300 rounded"
                value={bankCode}
                onChange={(e) => setBankCode(e.target.value)}
              >
                <option value="BC010">BC010 - Barclays Current</option>
                <option value="BC020">BC020 - Barclays Clearing</option>
                <option value="BC026">BC026 - Tide Current</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Posting Date</label>
              <input
                type="date"
                className="w-full p-2 border border-gray-300 rounded"
                value={postDate}
                onChange={(e) => setPostDate(e.target.value)}
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Batch Status</label>
              <select
                className="w-full p-2 border border-gray-300 rounded"
                value={completeBatch ? 'complete' : 'review'}
                onChange={(e) => setCompleteBatch(e.target.value === 'complete')}
              >
                <option value="review">Leave for Review (incomplete)</option>
                <option value="complete">Complete Immediately</option>
              </select>
            </div>
          </div>

          {unmatchedCount > 0 && (
            <div className="mb-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg flex items-start gap-2">
              <AlertCircle className="h-5 w-5 text-yellow-500 flex-shrink-0 mt-0.5" />
              <div className="text-sm">
                <p className="font-medium text-yellow-800">
                  {unmatchedCount} payment(s) don't have customer accounts assigned
                </p>
                <p className="text-yellow-700">
                  These will be skipped. Please select Opera accounts above to include them.
                </p>
              </div>
            </div>
          )}

          {importResult && (
            <div className={`mb-4 p-3 rounded-lg flex items-start gap-2 ${
              importResult.success ? 'bg-green-50 border border-green-200' : 'bg-red-50 border border-red-200'
            }`}>
              {importResult.success ? (
                <CheckCircle className="h-5 w-5 text-green-500 flex-shrink-0 mt-0.5" />
              ) : (
                <AlertCircle className="h-5 w-5 text-red-500 flex-shrink-0 mt-0.5" />
              )}
              <p className={`text-sm font-medium ${importResult.success ? 'text-green-800' : 'text-red-800'}`}>
                {importResult.message}
              </p>
            </div>
          )}

          <div className="flex justify-end">
            <button
              onClick={handleImport}
              disabled={isImporting || matchedCount === 0}
              className="flex items-center gap-2 px-6 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
            >
              {isImporting ? (
                <>
                  <div className="animate-spin h-4 w-4 border-2 border-white border-t-transparent rounded-full" />
                  Importing...
                </>
              ) : (
                <>
                  <Upload className="h-4 w-4" />
                  Import {matchedCount} Payment{matchedCount !== 1 ? 's' : ''} to Opera
                  <ArrowRight className="h-4 w-4" />
                </>
              )}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
