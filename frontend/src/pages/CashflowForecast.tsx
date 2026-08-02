/**
 * Cashflow forecast page — uses the new SAM cashflow plugin.
 *
 * Read-only forward view of the Opera company's cash position over
 * the next 12 months. Combines outstanding commitments (debtors /
 * creditors with due dates), scheduled recurring entries, and a
 * 12-month historical average baseline.
 *
 * The chart is an inline SVG (no chart library dependency).
 */
import { useEffect, useMemo, useState } from 'react';
import {
  TrendingUp,
  TrendingDown,
  Wallet,
  ArrowUpCircle,
  ArrowDownCircle,
  AlertTriangle,
  Info,
  RefreshCw,
  Building2,
} from 'lucide-react';
import apiClient, {
  type CashflowPluginForecastResponse,
  type CashflowPluginMonth,
} from '../api/client';

type Loadable<T> =
  | { state: 'idle' }
  | { state: 'loading' }
  | { state: 'ok'; value: T }
  | { state: 'error'; message: string };

const MONTHS_OPTIONS = [3, 6, 12, 18, 24] as const;

function formatGBP(value: number): string {
  const abs = Math.abs(value);
  const formatted = abs.toLocaleString('en-GB', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  });
  return value < 0 ? `-£${formatted}` : `£${formatted}`;
}

function formatGBPDetail(value: number): string {
  const abs = Math.abs(value);
  const formatted = abs.toLocaleString('en-GB', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
  return value < 0 ? `-£${formatted}` : `£${formatted}`;
}

interface ChartProps {
  months: CashflowPluginMonth[];
}

function CashflowChart({ months }: ChartProps) {
  // Layout
  const width = 760;
  const height = 320;
  const padding = { top: 24, right: 56, bottom: 56, left: 64 };
  const innerW = width - padding.left - padding.right;
  const innerH = height - padding.top - padding.bottom;

  // Scales
  const maxBar = Math.max(
    1,
    ...months.map((m) =>
      Math.max(m.expected_receipts, m.expected_payments),
    ),
  );
  const minBalance = Math.min(0, ...months.map((m) => m.running_balance));
  const maxBalance = Math.max(0, ...months.map((m) => m.running_balance));
  const balanceRange = Math.max(1, maxBalance - minBalance);

  const barCount = months.length;
  const slotW = barCount > 0 ? innerW / barCount : innerW;
  const barW = Math.max(4, slotW * 0.35);

  const xForBar = (i: number, offset: 0 | 1) =>
    padding.left + slotW * i + (slotW - barW * 2) / 2 + barW * offset;

  const yForBar = (val: number) =>
    padding.top + innerH - (val / maxBar) * innerH;

  const xForLine = (i: number) =>
    padding.left + slotW * i + slotW / 2;
  const yForLine = (val: number) =>
    padding.top +
    innerH -
    ((val - minBalance) / balanceRange) * innerH;

  // Y-axis ticks for bars (left)
  const tickCount = 4;
  const tickValues = Array.from(
    { length: tickCount + 1 },
    (_, i) => (maxBar * i) / tickCount,
  );

  return (
    <div className="w-full overflow-x-auto">
      <svg
        width={width}
        height={height}
        viewBox={`0 0 ${width} ${height}`}
        className="block min-w-[640px]"
        role="img"
        aria-label="Cashflow forecast chart"
      >
        {/* Y-axis grid + labels (left = receipts/payments) */}
        {tickValues.map((tv, idx) => {
          const y = yForBar(tv);
          return (
            <g key={`grid-${idx}`}>
              <line
                x1={padding.left}
                x2={width - padding.right}
                y1={y}
                y2={y}
                stroke="#e5e7eb"
                strokeDasharray={idx === 0 ? '0' : '2 3'}
                strokeWidth={1}
              />
              <text
                x={padding.left - 8}
                y={y + 4}
                textAnchor="end"
                fontSize={11}
                fill="#6b7280"
              >
                {formatGBP(tv)}
              </text>
            </g>
          );
        })}
        {/* Zero line for the balance series (right axis) */}
        {minBalance < 0 && (
          <line
            x1={padding.left}
            x2={width - padding.right}
            y1={yForLine(0)}
            y2={yForLine(0)}
            stroke="#fca5a5"
            strokeDasharray="3 4"
            strokeWidth={1}
          />
        )}

        {/* Bars */}
        {months.map((m, i) => {
          const rY = yForBar(m.expected_receipts);
          const pY = yForBar(m.expected_payments);
          const rH = padding.top + innerH - rY;
          const pH = padding.top + innerH - pY;
          return (
            <g key={`bar-${m.month}`}>
              <rect
                x={xForBar(i, 0)}
                y={rY}
                width={barW}
                height={Math.max(0, rH)}
                fill="#10b981"
                opacity={0.85}
              >
                <title>
                  {m.label} — Receipts {formatGBPDetail(m.expected_receipts)}
                </title>
              </rect>
              <rect
                x={xForBar(i, 1)}
                y={pY}
                width={barW}
                height={Math.max(0, pH)}
                fill="#ef4444"
                opacity={0.85}
              >
                <title>
                  {m.label} — Payments {formatGBPDetail(m.expected_payments)}
                </title>
              </rect>
            </g>
          );
        })}

        {/* Running-balance line */}
        <polyline
          points={months
            .map((m, i) => `${xForLine(i)},${yForLine(m.running_balance)}`)
            .join(' ')}
          fill="none"
          stroke="#3b82f6"
          strokeWidth={2.5}
        />
        {months.map((m, i) => (
          <g key={`pt-${m.month}`}>
            <circle
              cx={xForLine(i)}
              cy={yForLine(m.running_balance)}
              r={3.5}
              fill="#3b82f6"
            >
              <title>
                {m.label} — Running balance {formatGBPDetail(m.running_balance)}
              </title>
            </circle>
          </g>
        ))}

        {/* X-axis labels */}
        {months.map((m, i) => (
          <text
            key={`x-${m.month}`}
            x={xForLine(i)}
            y={height - padding.bottom + 18}
            textAnchor="middle"
            fontSize={11}
            fill="#374151"
          >
            {m.label.split(' ')[0]}
          </text>
        ))}
        {months.length > 0 && (
          <text
            x={padding.left + innerW / 2}
            y={height - padding.bottom + 36}
            textAnchor="middle"
            fontSize={11}
            fill="#9ca3af"
          >
            {months[0]?.label} → {months[months.length - 1]?.label}
          </text>
        )}

        {/* Legend */}
        <g transform={`translate(${padding.left}, ${padding.top - 12})`}>
          <rect width={10} height={10} fill="#10b981" />
          <text x={14} y={9} fontSize={11} fill="#374151">
            Receipts
          </text>
          <rect x={84} width={10} height={10} fill="#ef4444" />
          <text x={98} y={9} fontSize={11} fill="#374151">
            Payments
          </text>
          <line
            x1={170}
            x2={188}
            y1={5}
            y2={5}
            stroke="#3b82f6"
            strokeWidth={2.5}
          />
          <text x={192} y={9} fontSize={11} fill="#374151">
            Running balance
          </text>
        </g>
      </svg>
    </div>
  );
}

export default function CashflowForecast() {
  const [monthsAhead, setMonthsAhead] = useState<number>(12);
  const [state, setState] =
    useState<Loadable<CashflowPluginForecastResponse>>({ state: 'idle' });

  useEffect(() => {
    let cancelled = false;
    setState({ state: 'loading' });
    apiClient
      .cashflowPluginForecast(undefined, monthsAhead)
      .then((res) => {
        if (cancelled) return;
        if (res.data.success) {
          setState({ state: 'ok', value: res.data });
        } else {
          setState({
            state: 'error',
            message: res.data.error ?? 'Forecast failed',
          });
        }
      })
      .catch((err) => {
        if (cancelled) return;
        setState({
          state: 'error',
          message: err?.message ?? String(err),
        });
      });
    return () => {
      cancelled = true;
    };
  }, [monthsAhead]);

  const data = state.state === 'ok' ? state.value : null;

  const totalsLine = useMemo(() => {
    if (!data) return null;
    const opening = data.totals.opening_balance;
    const closing = data.totals.closing_balance;
    const delta = closing - opening;
    return { opening, closing, delta };
  }, [data]);

  const lowestBalanceWarning = useMemo(() => {
    if (!data) return null;
    if (data.totals.lowest_balance >= 0) return null;
    return data.totals.lowest_balance_month
      ? `Bank balance projected to dip to ${formatGBPDetail(
          data.totals.lowest_balance,
        )} in ${data.totals.lowest_balance_month}.`
      : `Bank balance projected to go negative (low point ${formatGBPDetail(
          data.totals.lowest_balance,
        )}).`;
  }, [data]);

  return (
    <div className="p-6 space-y-6 max-w-6xl mx-auto">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900">
            Cashflow Forecast
          </h1>
          <p className="text-sm text-gray-600 mt-1">
            Forward cashflow view driven by Opera commitments, recurring entries
            and 12-month historical averages.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <label className="text-sm text-gray-600">Horizon</label>
          <select
            value={monthsAhead}
            onChange={(e) => setMonthsAhead(Number(e.target.value))}
            className="px-3 py-1.5 text-sm border border-gray-300 rounded-md bg-white focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {MONTHS_OPTIONS.map((n) => (
              <option key={n} value={n}>
                {n} months
              </option>
            ))}
          </select>
          <button
            onClick={() => setMonthsAhead((m) => m)}
            className="p-1.5 text-gray-500 hover:text-gray-700 rounded-md hover:bg-gray-100"
            title="Refresh"
          >
            <RefreshCw size={16} />
          </button>
        </div>
      </div>

      {state.state === 'loading' && (
        <div className="bg-white rounded-lg border border-gray-200 p-8 text-center text-gray-500">
          Loading forecast…
        </div>
      )}

      {state.state === 'error' && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 flex items-start gap-3">
          <AlertTriangle className="text-red-500 flex-shrink-0 mt-0.5" size={18} />
          <div>
            <p className="text-sm font-medium text-red-800">
              Could not load forecast
            </p>
            <p className="text-sm text-red-700 mt-1">{state.message}</p>
          </div>
        </div>
      )}

      {data && (
        <>
          {/* Current position cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
            <PositionCard
              icon={<Wallet size={18} className="text-blue-500" />}
              label="Bank balance"
              value={data.current_position.bank_total}
              sublabel={`${data.current_position.bank_accounts.length} account(s)`}
            />
            <PositionCard
              icon={<ArrowUpCircle size={18} className="text-emerald-500" />}
              label="Debtors outstanding"
              value={data.current_position.debtors_outstanding}
              sublabel="Customers owe us"
            />
            <PositionCard
              icon={<ArrowDownCircle size={18} className="text-rose-500" />}
              label="Creditors outstanding"
              value={data.current_position.creditors_outstanding}
              sublabel="We owe suppliers"
            />
            <PositionCard
              icon={
                data.current_position.net_working_capital >= 0 ? (
                  <TrendingUp size={18} className="text-emerald-500" />
                ) : (
                  <TrendingDown size={18} className="text-rose-500" />
                )
              }
              label="Net working capital"
              value={data.current_position.net_working_capital}
              sublabel="Bank + Debtors − Creditors"
            />
          </div>

          {lowestBalanceWarning && (
            <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 flex items-start gap-2">
              <AlertTriangle
                size={18}
                className="text-amber-500 flex-shrink-0 mt-0.5"
              />
              <p className="text-sm text-amber-800">{lowestBalanceWarning}</p>
            </div>
          )}

          {/* Chart */}
          <div className="bg-white rounded-lg border border-gray-200 p-4">
            <CashflowChart months={data.monthly_forecast} />
          </div>

          {/* Summary line */}
          {totalsLine && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 flex items-center gap-6 text-sm">
              <div>
                <span className="text-gray-600">Opening</span>{' '}
                <span className="font-semibold text-gray-900">
                  {formatGBPDetail(totalsLine.opening)}
                </span>
              </div>
              <div className="text-gray-400">→</div>
              <div>
                <span className="text-gray-600">Closing</span>{' '}
                <span
                  className={
                    totalsLine.closing >= 0
                      ? 'font-semibold text-emerald-700'
                      : 'font-semibold text-rose-700'
                  }
                >
                  {formatGBPDetail(totalsLine.closing)}
                </span>
              </div>
              <div className="text-gray-400">·</div>
              <div>
                <span className="text-gray-600">Net change</span>{' '}
                <span
                  className={
                    totalsLine.delta >= 0
                      ? 'font-semibold text-emerald-700'
                      : 'font-semibold text-rose-700'
                  }
                >
                  {totalsLine.delta >= 0 ? '+' : ''}
                  {formatGBPDetail(totalsLine.delta)}
                </span>
              </div>
            </div>
          )}

          {/* Monthly table */}
          <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 text-left text-xs uppercase text-gray-600">
                  <th className="px-4 py-2 font-medium">Month</th>
                  <th className="px-4 py-2 font-medium text-right">Receipts</th>
                  <th className="px-4 py-2 font-medium text-right">Payments</th>
                  <th className="px-4 py-2 font-medium text-right">Net</th>
                  <th className="px-4 py-2 font-medium text-right">
                    Running balance
                  </th>
                </tr>
              </thead>
              <tbody>
                {data.monthly_forecast.map((m) => (
                  <tr
                    key={m.month}
                    className="border-t border-gray-100 hover:bg-gray-50"
                  >
                    <td className="px-4 py-2 font-medium text-gray-900">
                      {m.label}
                    </td>
                    <td className="px-4 py-2 text-right text-emerald-700">
                      {formatGBPDetail(m.expected_receipts)}
                    </td>
                    <td className="px-4 py-2 text-right text-rose-700">
                      {formatGBPDetail(m.expected_payments)}
                    </td>
                    <td
                      className={`px-4 py-2 text-right font-medium ${
                        m.net_cashflow >= 0
                          ? 'text-emerald-700'
                          : 'text-rose-700'
                      }`}
                    >
                      {m.net_cashflow >= 0 ? '+' : ''}
                      {formatGBPDetail(m.net_cashflow)}
                    </td>
                    <td
                      className={`px-4 py-2 text-right font-medium ${
                        m.running_balance >= 0
                          ? 'text-gray-900'
                          : 'text-rose-700'
                      }`}
                    >
                      {formatGBPDetail(m.running_balance)}
                    </td>
                  </tr>
                ))}
                <tr className="border-t-2 border-gray-200 bg-gray-50 font-semibold text-gray-900">
                  <td className="px-4 py-2">Total</td>
                  <td className="px-4 py-2 text-right text-emerald-700">
                    {formatGBPDetail(data.totals.total_receipts)}
                  </td>
                  <td className="px-4 py-2 text-right text-rose-700">
                    {formatGBPDetail(data.totals.total_payments)}
                  </td>
                  <td
                    className={`px-4 py-2 text-right ${
                      data.totals.net_position >= 0
                        ? 'text-emerald-700'
                        : 'text-rose-700'
                    }`}
                  >
                    {data.totals.net_position >= 0 ? '+' : ''}
                    {formatGBPDetail(data.totals.net_position)}
                  </td>
                  <td
                    className={`px-4 py-2 text-right ${
                      data.totals.closing_balance >= 0
                        ? 'text-gray-900'
                        : 'text-rose-700'
                    }`}
                  >
                    {formatGBPDetail(data.totals.closing_balance)}
                  </td>
                </tr>
              </tbody>
            </table>
          </div>

          {/* Bank account breakdown */}
          {data.current_position.bank_accounts.length > 0 && (
            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <h2 className="text-sm font-semibold text-gray-900 mb-3 flex items-center gap-2">
                <Building2 size={16} className="text-gray-500" />
                Bank accounts ({data.current_position.bank_accounts.length})
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-2">
                {data.current_position.bank_accounts.map((b) => (
                  <div
                    key={b.code}
                    className="flex items-center justify-between border border-gray-100 rounded-md px-3 py-2 text-sm"
                  >
                    <div>
                      <div className="font-medium text-gray-900">{b.code}</div>
                      <div className="text-xs text-gray-500">
                        {b.description || '—'}
                      </div>
                    </div>
                    <div
                      className={`font-medium ${
                        b.balance >= 0 ? 'text-gray-900' : 'text-rose-700'
                      }`}
                    >
                      {formatGBPDetail(b.balance)}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Assumptions */}
          {data.assumptions.length > 0 && (
            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <h2 className="text-sm font-semibold text-gray-900 mb-2 flex items-center gap-2">
                <Info size={16} className="text-gray-500" />
                How this forecast is built
              </h2>
              <ul className="text-sm text-gray-600 space-y-1.5 list-disc list-inside">
                {data.assumptions.map((a, i) => (
                  <li key={i}>{a}</li>
                ))}
              </ul>
            </div>
          )}
        </>
      )}
    </div>
  );
}

interface PositionCardProps {
  icon: React.ReactNode;
  label: string;
  value: number;
  sublabel?: string;
}

function PositionCard({ icon, label, value, sublabel }: PositionCardProps) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-3">
      <div className="flex items-center gap-2 text-xs text-gray-600">
        {icon}
        <span>{label}</span>
      </div>
      <div
        className={`mt-1 text-xl font-semibold ${
          value >= 0 ? 'text-gray-900' : 'text-rose-700'
        }`}
      >
        {formatGBPDetail(value)}
      </div>
      {sublabel && (
        <div className="mt-0.5 text-xs text-gray-500">{sublabel}</div>
      )}
    </div>
  );
}
