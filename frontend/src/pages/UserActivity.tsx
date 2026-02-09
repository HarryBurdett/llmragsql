import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Users,
  RefreshCw,
  Calendar,
  Clock,
  TrendingUp,
  FileText,
  BarChart3,
  ChevronDown,
  ChevronRight,
  Filter,
} from 'lucide-react';
import axios from 'axios';

interface TypeBreakdown {
  count: number;
  total_value: number;
}

interface UserActivityData {
  user_code: string;
  nominal_transactions: number;
  cashbook_entries: number;
  total_debits: number;
  total_credits: number;
  first_activity: string;
  last_activity: string;
  by_type: Record<string, TypeBreakdown>;
}

interface DailyActivity {
  date: string;
  count: number;
}

interface HourlyData {
  hour: number;
  label: string;
  count: number;
}

interface UserActivityResponse {
  success: boolean;
  period: {
    start_date: string;
    end_date: string;
  };
  users: UserActivityData[];
  summary: {
    total_users: number;
    total_transactions: number;
    by_type: Record<string, TypeBreakdown>;
  };
  daily_activity: DailyActivity[];
  hourly_distribution: HourlyData[];
}

interface UserListResponse {
  success: boolean;
  count: number;
  users: string[];
}

export function UserActivity() {
  const [startDate, setStartDate] = useState(() => {
    const d = new Date();
    d.setDate(d.getDate() - 30);
    return d.toISOString().split('T')[0];
  });
  const [endDate, setEndDate] = useState(() => new Date().toISOString().split('T')[0]);
  const [selectedUser, setSelectedUser] = useState<string>('');
  const [expandedUsers, setExpandedUsers] = useState<Set<string>>(new Set());

  // Fetch available users
  const usersQuery = useQuery<UserListResponse>({
    queryKey: ['userActivityUsers'],
    queryFn: async () => {
      const response = await axios.get<UserListResponse>('/api/user-activity/users');
      return response.data;
    },
  });

  // Fetch activity data
  const activityQuery = useQuery<UserActivityResponse>({
    queryKey: ['userActivity', startDate, endDate, selectedUser],
    queryFn: async () => {
      const params = new URLSearchParams();
      params.append('start_date', startDate);
      params.append('end_date', endDate);
      if (selectedUser) {
        params.append('user_filter', selectedUser);
      }
      const response = await axios.get<UserActivityResponse>(`/api/user-activity?${params.toString()}`);
      return response.data;
    },
    refetchOnWindowFocus: false,
  });

  const toggleUserExpanded = (userCode: string) => {
    const newExpanded = new Set(expandedUsers);
    if (newExpanded.has(userCode)) {
      newExpanded.delete(userCode);
    } else {
      newExpanded.add(userCode);
    }
    setExpandedUsers(newExpanded);
  };

  const formatCurrency = (value: number | undefined | null) => {
    if (value === undefined || value === null) return '£0.00';
    return new Intl.NumberFormat('en-GB', {
      style: 'currency',
      currency: 'GBP',
    }).format(value);
  };

  const formatDateTime = (dateStr: string | undefined) => {
    if (!dateStr) return '-';
    const date = new Date(dateStr);
    return date.toLocaleString('en-GB', {
      day: '2-digit',
      month: 'short',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  };

  const formatDate = (dateStr: string | undefined) => {
    if (!dateStr) return '-';
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-GB', {
      day: '2-digit',
      month: 'short',
    });
  };

  const data = activityQuery.data;
  const isLoading = activityQuery.isLoading;

  // Calculate max for hourly chart scaling
  const maxHourlyCount = Math.max(...(data?.hourly_distribution?.map(h => h.count) || [1]));

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-gradient-to-r from-indigo-600 to-blue-600 rounded-xl shadow-lg p-6 text-white">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold flex items-center gap-3">
              <div className="p-2 bg-white/20 rounded-lg backdrop-blur-sm">
                <Users className="h-6 w-6" />
              </div>
              User Activity Monitor
            </h1>
            <p className="text-indigo-100 mt-2">
              Track user efficiency and transaction activity
            </p>
          </div>
          <button
            onClick={() => activityQuery.refetch()}
            disabled={isLoading}
            className="flex items-center gap-2 px-4 py-2 bg-white/20 hover:bg-white/30 backdrop-blur-sm rounded-lg transition-colors disabled:opacity-50"
          >
            <RefreshCw className={`h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="flex items-center gap-4 flex-wrap">
          <div className="flex items-center gap-2">
            <Calendar className="h-4 w-4 text-gray-500" />
            <label className="text-sm font-medium text-gray-700">From:</label>
            <input
              type="date"
              value={startDate}
              onChange={(e) => setStartDate(e.target.value)}
              className="px-3 py-1.5 border border-gray-300 rounded-md text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            />
          </div>
          <div className="flex items-center gap-2">
            <label className="text-sm font-medium text-gray-700">To:</label>
            <input
              type="date"
              value={endDate}
              onChange={(e) => setEndDate(e.target.value)}
              className="px-3 py-1.5 border border-gray-300 rounded-md text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            />
          </div>
          <div className="flex items-center gap-2">
            <Filter className="h-4 w-4 text-gray-500" />
            <label className="text-sm font-medium text-gray-700">User:</label>
            <select
              value={selectedUser}
              onChange={(e) => setSelectedUser(e.target.value)}
              className="px-3 py-1.5 border border-gray-300 rounded-md text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            >
              <option value="">All Users</option>
              {usersQuery.data?.users?.map((user) => (
                <option key={user} value={user}>
                  {user}
                </option>
              ))}
            </select>
          </div>
          <div className="flex-1" />
          <div className="text-sm text-gray-500">
            {data?.period && (
              <span>
                Showing {data.period.start_date} to {data.period.end_date}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Loading State */}
      {isLoading && (
        <div className="bg-white rounded-lg shadow p-8 text-center">
          <RefreshCw className="h-8 w-8 animate-spin text-indigo-600 mx-auto mb-4" />
          <p className="text-gray-600">Loading user activity data...</p>
        </div>
      )}

      {/* Data Display */}
      {data && !isLoading && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-4 gap-4">
            <div className="bg-white rounded-lg shadow p-4">
              <div className="flex items-center gap-2 text-indigo-600 mb-2">
                <Users className="h-5 w-5" />
                <span className="text-sm font-medium">Active Users</span>
              </div>
              <p className="text-3xl font-bold text-gray-900">{data.summary.total_users}</p>
              <p className="text-xs text-gray-500 mt-1">In selected period</p>
            </div>

            <div className="bg-white rounded-lg shadow p-4">
              <div className="flex items-center gap-2 text-green-600 mb-2">
                <TrendingUp className="h-5 w-5" />
                <span className="text-sm font-medium">Total Transactions</span>
              </div>
              <p className="text-3xl font-bold text-gray-900">
                {data.summary.total_transactions.toLocaleString()}
              </p>
              <p className="text-xs text-gray-500 mt-1">All types combined</p>
            </div>

            <div className="bg-white rounded-lg shadow p-4">
              <div className="flex items-center gap-2 text-blue-600 mb-2">
                <FileText className="h-5 w-5" />
                <span className="text-sm font-medium">Transaction Types</span>
              </div>
              <p className="text-3xl font-bold text-gray-900">
                {Object.keys(data.summary.by_type).length}
              </p>
              <p className="text-xs text-gray-500 mt-1">Different categories</p>
            </div>

            <div className="bg-white rounded-lg shadow p-4">
              <div className="flex items-center gap-2 text-purple-600 mb-2">
                <BarChart3 className="h-5 w-5" />
                <span className="text-sm font-medium">Avg per User</span>
              </div>
              <p className="text-3xl font-bold text-gray-900">
                {data.summary.total_users > 0
                  ? Math.round(data.summary.total_transactions / data.summary.total_users).toLocaleString()
                  : 0}
              </p>
              <p className="text-xs text-gray-500 mt-1">Transactions per user</p>
            </div>
          </div>

          {/* Hourly Distribution */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
              <Clock className="h-5 w-5 text-indigo-600" />
              Activity by Hour of Day
            </h2>
            <div className="flex items-end gap-1 h-32">
              {Array.from({ length: 24 }, (_, i) => {
                const hourData = data.hourly_distribution.find(h => h.hour === i);
                const count = hourData?.count || 0;
                const height = maxHourlyCount > 0 ? (count / maxHourlyCount) * 100 : 0;
                return (
                  <div key={i} className="flex-1 flex flex-col items-center">
                    <div
                      className="w-full bg-indigo-500 rounded-t transition-all hover:bg-indigo-600"
                      style={{ height: `${height}%`, minHeight: count > 0 ? '4px' : '0' }}
                      title={`${i}:00 - ${count} transactions`}
                    />
                    <span className="text-xs text-gray-400 mt-1">
                      {i % 4 === 0 ? `${i}` : ''}
                    </span>
                  </div>
                );
              })}
            </div>
            <div className="flex justify-between text-xs text-gray-500 mt-2">
              <span>Midnight</span>
              <span>6am</span>
              <span>Noon</span>
              <span>6pm</span>
              <span>Midnight</span>
            </div>
          </div>

          {/* Daily Activity */}
          {data.daily_activity.length > 0 && (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
                <Calendar className="h-5 w-5 text-indigo-600" />
                Daily Activity
              </h2>
              <div className="flex items-end gap-1 h-24">
                {data.daily_activity.map((day, idx) => {
                  const maxDaily = Math.max(...data.daily_activity.map(d => d.count));
                  const height = maxDaily > 0 ? (day.count / maxDaily) * 100 : 0;
                  return (
                    <div key={idx} className="flex-1 flex flex-col items-center">
                      <div
                        className="w-full bg-green-500 rounded-t transition-all hover:bg-green-600"
                        style={{ height: `${height}%`, minHeight: day.count > 0 ? '4px' : '0' }}
                        title={`${day.date} - ${day.count} transactions`}
                      />
                    </div>
                  );
                })}
              </div>
              <div className="flex justify-between text-xs text-gray-500 mt-2">
                <span>{formatDate(data.daily_activity[0]?.date)}</span>
                <span>{formatDate(data.daily_activity[data.daily_activity.length - 1]?.date)}</span>
              </div>
            </div>
          )}

          {/* User Table */}
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="p-4 border-b bg-gray-50">
              <h2 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                <Users className="h-5 w-5 text-indigo-600" />
                User Breakdown
              </h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                    <th className="text-left py-3 px-4 font-semibold w-8"></th>
                    <th className="text-left py-3 px-4 font-semibold">User</th>
                    <th className="text-right py-3 px-4 font-semibold">NL Transactions</th>
                    <th className="text-right py-3 px-4 font-semibold">Cashbook</th>
                    <th className="text-right py-3 px-4 font-semibold">Total Debits</th>
                    <th className="text-right py-3 px-4 font-semibold">Total Credits</th>
                    <th className="text-left py-3 px-4 font-semibold">First Activity</th>
                    <th className="text-left py-3 px-4 font-semibold">Last Activity</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {data.users.map((user) => (
                    <>
                      <tr
                        key={user.user_code}
                        className="hover:bg-gray-50 transition-colors cursor-pointer"
                        onClick={() => toggleUserExpanded(user.user_code)}
                      >
                        <td className="py-3 px-4">
                          {expandedUsers.has(user.user_code) ? (
                            <ChevronDown className="h-4 w-4 text-gray-400" />
                          ) : (
                            <ChevronRight className="h-4 w-4 text-gray-400" />
                          )}
                        </td>
                        <td className="py-3 px-4 font-mono font-medium text-indigo-600">
                          {user.user_code}
                        </td>
                        <td className="py-3 px-4 text-right font-semibold">
                          {user.nominal_transactions.toLocaleString()}
                        </td>
                        <td className="py-3 px-4 text-right">
                          {user.cashbook_entries.toLocaleString()}
                        </td>
                        <td className="py-3 px-4 text-right text-green-600">
                          {formatCurrency(user.total_debits)}
                        </td>
                        <td className="py-3 px-4 text-right text-red-600">
                          {formatCurrency(user.total_credits)}
                        </td>
                        <td className="py-3 px-4 text-sm text-gray-600">
                          {formatDateTime(user.first_activity)}
                        </td>
                        <td className="py-3 px-4 text-sm text-gray-600">
                          {formatDateTime(user.last_activity)}
                        </td>
                      </tr>
                      {expandedUsers.has(user.user_code) && Object.keys(user.by_type).length > 0 && (
                        <tr key={`${user.user_code}-details`}>
                          <td colSpan={8} className="bg-gray-50 px-4 py-3">
                            <div className="ml-8">
                              <h4 className="text-sm font-medium text-gray-700 mb-2">
                                Transaction Types
                              </h4>
                              <div className="grid grid-cols-4 gap-3">
                                {Object.entries(user.by_type).map(([typeName, typeData]) => (
                                  <div
                                    key={typeName}
                                    className="bg-white rounded-lg border p-3"
                                  >
                                    <p className="text-xs font-medium text-gray-500 truncate">
                                      {typeName}
                                    </p>
                                    <p className="text-lg font-bold text-gray-900">
                                      {typeData.count.toLocaleString()}
                                    </p>
                                    <p className="text-xs text-gray-500">
                                      {formatCurrency(typeData.total_value)}
                                    </p>
                                  </div>
                                ))}
                              </div>
                            </div>
                          </td>
                        </tr>
                      )}
                    </>
                  ))}
                </tbody>
              </table>
            </div>
            {data.users.length === 0 && (
              <div className="p-8 text-center text-gray-500">
                No user activity found for the selected period
              </div>
            )}
          </div>

          {/* Transaction Types Summary */}
          {Object.keys(data.summary.by_type).length > 0 && (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
                <FileText className="h-5 w-5 text-indigo-600" />
                Transaction Types Summary
              </h2>
              <div className="grid grid-cols-4 gap-4">
                {Object.entries(data.summary.by_type)
                  .sort((a, b) => b[1].count - a[1].count)
                  .map(([typeName, typeData]) => (
                    <div
                      key={typeName}
                      className="bg-gray-50 rounded-lg p-4 border"
                    >
                      <p className="text-sm font-medium text-gray-700 truncate">
                        {typeName}
                      </p>
                      <p className="text-2xl font-bold text-gray-900 mt-1">
                        {typeData.count.toLocaleString()}
                      </p>
                      <p className="text-sm text-gray-500">
                        {formatCurrency(typeData.total_value)}
                      </p>
                    </div>
                  ))}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default UserActivity;
