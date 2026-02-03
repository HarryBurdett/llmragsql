import { useState } from 'react';
import { FileText, CheckCircle, XCircle, AlertCircle, Loader2, Receipt, CreditCard, FileSpreadsheet, BookOpen } from 'lucide-react';

interface ImportResult {
  success: boolean;
  validate_only: boolean;
  records_processed: number;
  records_imported: number;
  records_failed: number;
  errors: string[];
  details: string[];
}

const API_BASE = 'http://localhost:8000/api';

type ImportType = 'sales-receipt' | 'purchase-payment' | 'sales-invoice' | 'purchase-invoice' | 'nominal-journal';

export function Imports() {
  const [activeType, setActiveType] = useState<ImportType>('sales-receipt');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<ImportResult | null>(null);
  const [validateOnly, setValidateOnly] = useState(true);

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
        <h1 className="text-2xl font-bold text-gray-900">Opera Imports</h1>
        <p className="text-gray-600 mt-1">Import transactions into Opera SQL SE</p>
      </div>

      {/* Import Type Selector */}
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

      {/* Form */}
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

      {/* Results */}
      {result && (
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

      {/* Help Section */}
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
    </div>
  );
}
