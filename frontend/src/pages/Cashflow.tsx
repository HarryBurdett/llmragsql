import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  TrendingUp, TrendingDown, DollarSign, Calendar, RefreshCw,
  ArrowUpCircle, ArrowDownCircle, Wallet, Building2, ChevronDown, ChevronUp
} from 'lucide-react';
import apiClient from '../api/client';

export function Cashflow() {
  const [yearsHistory, setYearsHistory] = useState(3);
  const [showHistory, setShowHistory] = useState(false);

  // Fetch forecast data
  const forecastQuery = useQuery({
    queryKey: ['cashflowForecast', yearsHistory],
    queryFn: () => apiClient.cashflowForecast(yearsHistory),
  });

  // Fetch history data
  const historyQuery = useQuery({
    queryKey: ['cashflowHistory'],
    queryFn: () => apiClient.cashflowHistory(),
    enabled: showHistory,
  });

  const forecastData = forecastQuery.data?.data;
  const forecast = forecastData?.success ? forecastData : null;
  const forecastError = forecastData?.error;
  const historyData = historyQuery.data?.data;
  const history = historyData?.success ? historyData : null;

  const formatCurrency = (value: number): string => {
    return `£${Math.abs(value).toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  };

  const formatCurrencyWithSign = (value: number): string => {
    const formatted = formatCurrency(value);
    return value >= 0 ? `+${formatted}` : `-${formatted.substring(1)}`;
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'actual':
        return 'bg-green-50 border-green-200';
      case 'current':
        return 'bg-blue-50 border-blue-200';
      default:
        return 'bg-gray-50 border-gray-200';
    }
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'actual':
        return <span className="px-2 py-0.5 bg-green-100 text-green-700 rounded text-xs">Actual</span>;
      case 'current':
        return <span className="px-2 py-0.5 bg-blue-100 text-blue-700 rounded text-xs">Current</span>;
      default:
        return <span className="px-2 py-0.5 bg-gray-100 text-gray-700 rounded text-xs">Forecast</span>;
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Cashflow Forecast</h2>
          <p className="text-gray-600 mt-1">
            Predicted monthly cashflow based on {yearsHistory} years of historical data
          </p>
        </div>
        <div className="flex gap-2 items-center">
          <label className="text-sm text-gray-600">
            Years of history:
            <select
              value={yearsHistory}
              onChange={(e) => setYearsHistory(Number(e.target.value))}
              className="ml-2 input w-20"
            >
              <option value={1}>1</option>
              <option value={2}>2</option>
              <option value={3}>3</option>
              <option value={5}>5</option>
            </select>
          </label>
          <button
            onClick={() => forecastQuery.refetch()}
            disabled={forecastQuery.isLoading}
            className="btn btn-secondary flex items-center"
          >
            <RefreshCw className={`h-4 w-4 mr-2 ${forecastQuery.isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {forecastQuery.isLoading ? (
        <div className="flex items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 animate-spin text-blue-500" />
          <span className="ml-3 text-gray-600">Calculating forecast...</span>
        </div>
      ) : forecast ? (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {/* Annual Expected Receipts */}
            <div className="card bg-gradient-to-br from-green-500 to-green-600 text-white">
              <div className="flex items-center justify-between">
                <ArrowUpCircle className="h-8 w-8 opacity-80" />
                <Calendar className="h-5 w-5 opacity-60" />
              </div>
              <div className="mt-3">
                <p className="text-sm opacity-80">Expected Receipts ({forecast.forecast_year})</p>
                <p className="text-2xl font-bold">{formatCurrency(forecast.summary.annual_expected_receipts)}</p>
              </div>
            </div>

            {/* Annual Expected Payments */}
            <div className="card bg-gradient-to-br from-red-500 to-red-600 text-white">
              <div className="flex items-center justify-between">
                <ArrowDownCircle className="h-8 w-8 opacity-80" />
                <Calendar className="h-5 w-5 opacity-60" />
              </div>
              <div className="mt-3">
                <p className="text-sm opacity-80">Expected Payments ({forecast.forecast_year})</p>
                <p className="text-2xl font-bold">{formatCurrency(forecast.summary.annual_expected_payments)}</p>
              </div>
            </div>

            {/* Net Cashflow */}
            <div className={`card bg-gradient-to-br ${forecast.summary.annual_expected_net >= 0 ? 'from-blue-500 to-blue-600' : 'from-orange-500 to-orange-600'} text-white`}>
              <div className="flex items-center justify-between">
                {forecast.summary.annual_expected_net >= 0 ? (
                  <TrendingUp className="h-8 w-8 opacity-80" />
                ) : (
                  <TrendingDown className="h-8 w-8 opacity-80" />
                )}
                <DollarSign className="h-5 w-5 opacity-60" />
              </div>
              <div className="mt-3">
                <p className="text-sm opacity-80">Expected Net Cashflow</p>
                <p className="text-2xl font-bold">{formatCurrencyWithSign(forecast.summary.annual_expected_net)}</p>
              </div>
            </div>

            {/* Current Bank Balance */}
            <div className="card bg-gradient-to-br from-purple-500 to-purple-600 text-white">
              <div className="flex items-center justify-between">
                <Building2 className="h-8 w-8 opacity-80" />
                <Wallet className="h-5 w-5 opacity-60" />
              </div>
              <div className="mt-3">
                <p className="text-sm opacity-80">Current Bank Balance</p>
                <p className="text-2xl font-bold">{formatCurrency(forecast.summary.current_bank_balance)}</p>
              </div>
            </div>
          </div>

          {/* Payment Breakdown */}
          {(forecast.summary.annual_payroll || forecast.summary.annual_recurring_expenses) && (
            <div className="card">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Annual Payment Breakdown</h3>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div className="p-4 bg-red-50 rounded-lg border border-red-200">
                  <p className="text-sm text-gray-600">Purchase Payments</p>
                  <p className="text-lg font-bold text-red-600">{formatCurrency(forecast.summary.annual_purchase_payments || 0)}</p>
                </div>
                <div className="p-4 bg-amber-50 rounded-lg border border-amber-200">
                  <p className="text-sm text-gray-600">Payroll (Net + ER NI)</p>
                  <p className="text-lg font-bold text-amber-600">{formatCurrency(forecast.summary.annual_payroll || 0)}</p>
                </div>
                <div className="p-4 bg-orange-50 rounded-lg border border-orange-200">
                  <p className="text-sm text-gray-600">Recurring Expenses</p>
                  <p className="text-lg font-bold text-orange-600">{formatCurrency(forecast.summary.annual_recurring_expenses || 0)}</p>
                </div>
                <div className="p-4 bg-gray-100 rounded-lg border border-gray-300">
                  <p className="text-sm text-gray-600">Total Payments</p>
                  <p className="text-lg font-bold text-gray-800">{formatCurrency(forecast.summary.annual_expected_payments)}</p>
                </div>
              </div>
            </div>
          )}

          {/* YTD Comparison */}
          <div className="card">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Year-to-Date Comparison</h3>
            <div className="grid grid-cols-3 gap-6">
              <div className="text-center p-4 bg-green-50 rounded-lg">
                <p className="text-sm text-gray-600">YTD Receipts (Actual)</p>
                <p className="text-xl font-bold text-green-600">{formatCurrency(forecast.summary.ytd_actual_receipts)}</p>
              </div>
              <div className="text-center p-4 bg-red-50 rounded-lg">
                <p className="text-sm text-gray-600">YTD Payments (Actual)</p>
                <p className="text-xl font-bold text-red-600">{formatCurrency(forecast.summary.ytd_actual_payments)}</p>
              </div>
              <div className={`text-center p-4 rounded-lg ${forecast.summary.ytd_actual_net >= 0 ? 'bg-blue-50' : 'bg-orange-50'}`}>
                <p className="text-sm text-gray-600">YTD Net (Actual)</p>
                <p className={`text-xl font-bold ${forecast.summary.ytd_actual_net >= 0 ? 'text-blue-600' : 'text-orange-600'}`}>
                  {formatCurrencyWithSign(forecast.summary.ytd_actual_net)}
                </p>
              </div>
            </div>
          </div>

          {/* Monthly Forecast Table */}
          <div className="card">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">
              Monthly Forecast - {forecast.forecast_year}
            </h3>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Month</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Expected Receipts</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Expected Payments</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Net Cashflow</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Data Points</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {forecast.monthly_forecast.map((month) => (
                    <tr key={month.month} className={getStatusColor(month.status)}>
                      <td className="px-4 py-3 text-sm font-medium text-gray-900">
                        {month.month_name}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {getStatusBadge(month.status)}
                      </td>
                      <td className="px-4 py-3 text-sm text-right font-mono text-green-600">
                        {formatCurrency(month.expected_receipts)}
                      </td>
                      <td className="px-4 py-3 text-sm text-right font-mono text-red-600 group relative">
                        {formatCurrency(month.expected_payments)}
                        {(month.payroll || month.recurring_expenses) && (
                          <div className="hidden group-hover:block absolute right-0 top-full mt-1 z-10 bg-gray-900 text-white text-xs rounded-lg p-3 shadow-lg min-w-[200px]">
                            <div className="space-y-1">
                              <div className="flex justify-between">
                                <span>Purchases:</span>
                                <span>{formatCurrency(month.purchase_payments || 0)}</span>
                              </div>
                              <div className="flex justify-between">
                                <span>Payroll:</span>
                                <span>{formatCurrency(month.payroll || 0)}</span>
                              </div>
                              <div className="flex justify-between">
                                <span>Recurring:</span>
                                <span>{formatCurrency(month.recurring_expenses || 0)}</span>
                              </div>
                              <div className="border-t border-gray-700 pt-1 flex justify-between font-semibold">
                                <span>Total:</span>
                                <span>{formatCurrency(month.expected_payments)}</span>
                              </div>
                            </div>
                          </div>
                        )}
                      </td>
                      <td className={`px-4 py-3 text-sm text-right font-mono font-semibold ${
                        month.net_cashflow >= 0 ? 'text-blue-600' : 'text-orange-600'
                      }`}>
                        {formatCurrencyWithSign(month.net_cashflow)}
                      </td>
                      <td className="px-4 py-3 text-sm text-center text-gray-500">
                        R: {month.receipts_data_points} / P: {month.payments_data_points}
                      </td>
                    </tr>
                  ))}
                </tbody>
                <tfoot className="bg-gray-100">
                  <tr>
                    <td className="px-4 py-3 text-sm font-bold text-gray-900" colSpan={2}>
                      Annual Total
                    </td>
                    <td className="px-4 py-3 text-sm text-right font-mono font-bold text-green-700">
                      {formatCurrency(forecast.summary.annual_expected_receipts)}
                    </td>
                    <td className="px-4 py-3 text-sm text-right font-mono font-bold text-red-700">
                      {formatCurrency(forecast.summary.annual_expected_payments)}
                    </td>
                    <td className={`px-4 py-3 text-sm text-right font-mono font-bold ${
                      forecast.summary.annual_expected_net >= 0 ? 'text-blue-700' : 'text-orange-700'
                    }`}>
                      {formatCurrencyWithSign(forecast.summary.annual_expected_net)}
                    </td>
                    <td className="px-4 py-3"></td>
                  </tr>
                </tfoot>
              </table>
            </div>
          </div>

          {/* Bank Accounts */}
          {forecast.bank_accounts && forecast.bank_accounts.length > 0 && (
            <div className="card">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Bank Accounts</h3>
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
                {forecast.bank_accounts.map((account) => (
                  <div key={account.account} className="p-4 bg-gray-50 rounded-lg border border-gray-200">
                    <p className="text-xs text-gray-500 truncate">{account.description?.trim()}</p>
                    <p className="text-lg font-bold text-gray-900 mt-1">{formatCurrency(account.balance)}</p>
                    <p className="text-xs text-gray-400">{account.account}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Historical Data Toggle */}
          <div className="card">
            <button
              onClick={() => setShowHistory(!showHistory)}
              className="flex items-center justify-between w-full text-left"
            >
              <h3 className="text-lg font-semibold text-gray-900">Historical Data</h3>
              {showHistory ? <ChevronUp className="h-5 w-5" /> : <ChevronDown className="h-5 w-5" />}
            </button>

            {showHistory && (
              <div className="mt-4">
                {historyQuery.isLoading ? (
                  <div className="flex items-center justify-center py-8">
                    <RefreshCw className="h-6 w-6 animate-spin text-blue-500" />
                  </div>
                ) : history?.history && history.history.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-gray-200">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Year</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Total Receipts</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Total Payments</th>
                          <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Net Cashflow</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {history.history.map((year) => (
                          <tr key={year.year} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-sm font-medium text-gray-900">{year.year}</td>
                            <td className="px-4 py-3 text-sm text-right font-mono text-green-600">
                              {formatCurrency(year.total_receipts)}
                            </td>
                            <td className="px-4 py-3 text-sm text-right font-mono text-red-600">
                              {formatCurrency(year.total_payments)}
                            </td>
                            <td className={`px-4 py-3 text-sm text-right font-mono font-semibold ${
                              year.net_cashflow >= 0 ? 'text-blue-600' : 'text-orange-600'
                            }`}>
                              {formatCurrencyWithSign(year.net_cashflow)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="text-gray-500 text-center py-4">No historical data available</p>
                )}
              </div>
            )}
          </div>
        </>
      ) : (
        <div className="card text-center py-12">
          <DollarSign className="h-12 w-12 mx-auto text-gray-400 mb-4" />
          <p className="text-gray-600">Unable to load forecast data</p>
          {forecastQuery.error && (
            <p className="text-red-500 mt-2 text-sm">{String(forecastQuery.error)}</p>
          )}
          {forecastError && (
            <p className="text-red-500 mt-2 text-sm">{forecastError}</p>
          )}
        </div>
      )}
    </div>
  );
}
