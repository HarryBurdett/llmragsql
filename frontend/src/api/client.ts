import axios from 'axios';

const API_BASE_URL = 'http://localhost:8000/api';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Types
export interface Provider {
  id: string;
  name: string;
  requires_api_key: boolean;
}

export interface TableInfo {
  schema_name: string;
  table_name: string;
  table_type: string;
}

export interface ColumnInfo {
  column_name: string;
  data_type: string;
  is_nullable: boolean;
  column_default: string | null;
}

export interface SQLQueryResponse {
  success: boolean;
  data: Record<string, unknown>[];
  columns: string[];
  row_count: number;
  error?: string;
}

export interface RAGQueryResponse {
  success: boolean;
  answer: string;
  sources: { score: number; text: string }[];
  error?: string;
}

export interface SQLToRAGRequest {
  description: string;
  custom_sql?: string;
  table_filter?: string[];
  max_rows?: number;
}

export interface SQLToRAGResponse {
  success: boolean;
  message?: string;
  sql_used?: string;
  rows_ingested?: number;
  sample_data?: Record<string, unknown>[];
  error?: string;
}

export interface CreditControlQueryResponse {
  success: boolean;
  query_type: string;
  description: string;
  data: Record<string, unknown>[];
  count: number;
  summary: string;
  sql_used?: string;
  error?: string;
}

export interface DashboardMetric {
  value: number;
  count: number;
  label: string;
}

export interface PriorityAction {
  account: string;
  customer: string;
  balance: number;
  credit_limit: number;
  phone: string;
  contact: string;
  priority_reason: 'ON_STOP' | 'OVER_LIMIT' | 'HIGH_BALANCE';
}

export interface CreditControlDashboardResponse {
  success: boolean;
  metrics: {
    total_debt: DashboardMetric;
    over_credit_limit: DashboardMetric;
    accounts_on_stop: DashboardMetric;
    overdue_invoices: DashboardMetric;
    recent_payments: DashboardMetric;
    promises_due?: DashboardMetric;
    disputed?: DashboardMetric;
    unallocated_cash?: DashboardMetric;
  };
  priority_actions: PriorityAction[];
  error?: string;
}

export interface CreditControlQueryDefinition {
  name: string;
  category: string;
  description: string;
  keywords: string[];
  has_param_sql: boolean;
}

export interface DebtorRecord {
  account: string;
  customer: string;
  balance: number;
  current_period: number;
  month_1: number;
  month_2: number;
  month_3_plus: number;
  credit_limit: number;
  phone: string;
  contact: string;
  on_stop: boolean;
}

export interface DebtorsReportResponse {
  success: boolean;
  data: DebtorRecord[];
  count: number;
  totals: {
    balance: number;
    current: number;
    month_1: number;
    month_2: number;
    month_3_plus: number;
  };
  error?: string;
}

export interface TrialBalanceRecord {
  account_code: string;
  description: string;
  account_type: string;
  subtype: string;
  opening_balance: number;
  ytd_movement: number;
  debit: number;
  credit: number;
}

export interface TrialBalanceTypeSummary {
  name: string;
  debit: number;
  credit: number;
  count: number;
}

export interface TrialBalanceResponse {
  success: boolean;
  year: number;
  data: TrialBalanceRecord[];
  count: number;
  totals: {
    debit: number;
    credit: number;
    difference: number;
  };
  by_type: Record<string, TrialBalanceTypeSummary>;
  error?: string;
}

export interface StatutoryAccountItem {
  code: string;
  description: string;
  value: number;
}

export interface StatutoryAccountSection {
  items: StatutoryAccountItem[];
  total: number;
}

export interface StatutoryAccountsResponse {
  success: boolean;
  year: number;
  profit_and_loss: {
    turnover: StatutoryAccountSection;
    cost_of_sales: StatutoryAccountSection;
    gross_profit: number;
    administrative_expenses: StatutoryAccountSection;
    other_operating_income: StatutoryAccountSection;
    operating_profit: number;
    profit_before_tax: number;
    profit_after_tax: number;
  };
  balance_sheet: {
    fixed_assets: StatutoryAccountSection;
    current_assets: StatutoryAccountSection;
    current_liabilities: StatutoryAccountSection;
    net_current_assets: number;
    total_assets_less_current_liabilities: number;
    capital_and_reserves: StatutoryAccountSection;
  };
  error?: string;
}

export interface MonthlyForecast {
  month: number;
  month_name: string;
  expected_receipts: number;
  expected_payments: number;
  purchase_payments?: number;
  payroll?: number;
  recurring_expenses?: number;
  net_cashflow: number;
  receipts_data_points: number;
  payments_data_points: number;
  status: 'actual' | 'current' | 'forecast';
}

export interface BankAccount {
  account: string;
  description: string;
  balance: number;
}

export interface CashflowForecastResponse {
  success: boolean;
  forecast_year: number;
  years_of_history: number;
  monthly_forecast: MonthlyForecast[];
  summary: {
    annual_expected_receipts: number;
    annual_expected_payments: number;
    annual_purchase_payments?: number;
    annual_payroll?: number;
    annual_recurring_expenses?: number;
    annual_expected_net: number;
    ytd_actual_receipts: number;
    ytd_actual_payments: number;
    ytd_actual_net: number;
    current_bank_balance: number;
  };
  bank_accounts: BankAccount[];
  error?: string;
}

export interface YearlyHistory {
  year: number;
  total_receipts: number;
  total_payments: number;
  net_cashflow: number;
  monthly_receipts: Record<number, number>;
  monthly_payments: Record<number, number>;
}

export interface CashflowHistoryResponse {
  success: boolean;
  history: YearlyHistory[];
  error?: string;
}

export interface ProviderConfig {
  provider: string;
  api_key?: string;
  model: string;
  temperature: number;
  max_tokens: number;
  ollama_url?: string;  // For local Ollama running on network
}

export interface DatabaseConfig {
  type: string;
  server?: string;
  database?: string;
  username?: string;
  password?: string;
  use_windows_auth: boolean;
  // Advanced MS SQL settings
  pool_size?: number;
  max_overflow?: number;
  pool_timeout?: number;
  connection_timeout?: number;
  command_timeout?: number;
  ssl?: boolean;
  ssl_ca?: string;
  ssl_cert?: string;
  ssl_key?: string;
  port?: number;
}

// Sales Dashboard Types
export interface DashboardCeoKpisResponse {
  success: boolean;
  kpis: {
    mtd: number;
    qtd: number;
    ytd: number;
    yoy_growth_percent: number;
    avg_monthly_3m: number;
    avg_monthly_6m: number;
    avg_monthly_12m: number;
    active_customers: number;
    revenue_per_customer: number;
    year: number;
    month: number;
    quarter: number;
  };
}

export interface DashboardRevenueOverTimeResponse {
  success: boolean;
  year: number;
  months: {
    month: number;
    month_name: string;
    current_total: number;
    previous_total: number;
    categories: Record<string, number>;
  }[];
}

export interface DashboardRevenueCompositionResponse {
  success: boolean;
  year: number;
  current_total: number;
  previous_total: number;
  categories: {
    category: string;
    current_year: number;
    previous_year: number;
    current_percent: number;
    previous_percent: number;
    change_percent: number;
  }[];
}

export interface DashboardTopCustomersResponse {
  success: boolean;
  year: number;
  total_revenue: number;
  customers: {
    account_code: string;
    customer_name: string;
    current_year: number;
    previous_year: number;
    percent_of_total: number;
    cumulative_percent: number;
    invoice_count: number;
    trend: 'up' | 'down' | 'stable';
    change_percent: number;
  }[];
}

export interface DashboardCustomerConcentrationResponse {
  success: boolean;
  concentration: {
    total_customers: number;
    total_revenue: number;
    top_1_percent: number;
    top_3_percent: number;
    top_5_percent: number;
    top_10_percent: number;
    risk_level: 'low' | 'medium' | 'high';
  };
}

export interface DashboardCustomerLifecycleResponse {
  success: boolean;
  new_customers: number;
  lost_customers: number;
  age_bands: {
    less_than_1_year: { count: number; revenue: number };
    '1_to_3_years': { count: number; revenue: number };
    '3_to_5_years': { count: number; revenue: number };
    over_5_years: { count: number; revenue: number };
  };
}

export interface DashboardMarginByCategoryResponse {
  success: boolean;
  categories: {
    category: string;
    revenue: number;
    cost_of_sales: number;
    gross_profit: number;
    gross_margin_percent: number;
  }[];
  totals: {
    revenue: number;
    cost_of_sales: number;
    gross_profit: number;
    gross_margin_percent: number;
  };
}

// Creditors Control Types
export interface CreditorsDashboardMetric {
  value: number;
  count: number;
  label: string;
}

export interface TopSupplier {
  account: string;
  supplier: string;
  balance: number;
  phone: string;
  contact: string;
}

export interface CreditorsDashboardResponse {
  success: boolean;
  metrics: {
    total_creditors?: CreditorsDashboardMetric;
    overdue_invoices?: CreditorsDashboardMetric;
    due_7_days?: CreditorsDashboardMetric;
    due_30_days?: CreditorsDashboardMetric;
    recent_payments?: CreditorsDashboardMetric;
  };
  top_suppliers: TopSupplier[];
  error?: string;
}

export interface CreditorRecord {
  account: string;
  supplier: string;
  balance: number;
  current_period: number;
  month_1: number;
  month_2: number;
  month_3_plus: number;
  phone: string;
  contact: string;
}

export interface CreditorsReportResponse {
  success: boolean;
  data: CreditorRecord[];
  count: number;
  totals: {
    balance: number;
    current: number;
    month_1: number;
    month_2: number;
    month_3_plus: number;
  };
  error?: string;
}

export interface SupplierDetails {
  account: string;
  supplier_name: string;
  address1: string;
  address2: string;
  address3: string;
  address4: string;
  postcode: string;
  phone: string;
  contact: string;
  email: string;
  balance: number;
  turnover_ytd: number;
}

export interface SupplierTransaction {
  account: string;
  date: string;
  reference: string;
  type: string;
  description: string;
  value: number;
  balance: number;
  due_date: string;
  days_overdue: number;
}

export interface SupplierTransactionsResponse {
  success: boolean;
  transactions: SupplierTransaction[];
  count: number;
  summary: {
    total_invoices: number;
    total_credits: number;
    total_payments: number;
    balance: number;
  };
  error?: string;
}

export interface StatementTransaction {
  date: string;
  reference: string;
  type: string;
  description: string;
  debit: number;
  credit: number;
  balance: number;
  due_date: string;
  running_balance: number;
}

export interface SupplierStatementResponse {
  success: boolean;
  supplier: {
    account: string;
    supplier_name: string;
    address1: string;
    address2: string;
    address3: string;
    address4: string;
    postcode: string;
    current_balance: number;
  };
  period: {
    from_date: string;
    to_date: string;
  };
  opening_balance: number;
  transactions: StatementTransaction[];
  totals: {
    debits: number;
    credits: number;
    outstanding: number;
  };
  closing_balance: number;
  error?: string;
}

export interface SupplierSearchResult {
  account: string;
  supplier_name: string;
  balance: number;
  phone: string;
}

// API Functions
export const apiClient = {
  // Health & Status
  health: () => api.get('/health'),
  status: () => api.get('/status'),

  // Configuration
  getConfig: () => api.get('/config'),
  getProviders: () => api.get<{ providers: Provider[] }>('/config/providers'),
  getModels: (provider: string) => api.get<{ provider: string; models: string[] }>(`/config/models/${provider}`),
  updateLLMConfig: (config: ProviderConfig) => api.post('/config/llm', config),
  updateDatabaseConfig: (config: DatabaseConfig) => api.post('/config/database', config),

  // Database
  getTables: () => api.get<TableInfo[]>('/database/tables'),
  getColumns: (tableName: string, schemaName?: string) =>
    api.get<ColumnInfo[]>(`/database/tables/${tableName}/columns`, {
      params: { schema_name: schemaName || '' },
    }),
  executeQuery: (query: string, storeInVectorDb = false) =>
    api.post<SQLQueryResponse>('/database/query', { query, store_in_vector_db: storeInVectorDb }),

  // RAG
  ragQuery: (question: string, numResults = 5) =>
    api.post<RAGQueryResponse>('/rag/query', { question, num_results: numResults }),
  generateSQL: (question: string) =>
    api.post<{ success: boolean; sql: string; error?: string }>('/rag/generate-sql', null, {
      params: { question },
    }),
  getVectorStats: () => api.get('/rag/stats'),
  ingestData: (texts: string[], metadata?: Record<string, unknown>[]) =>
    api.post('/rag/ingest', { texts, metadata }),
  ingestFromSQL: (request: SQLToRAGRequest) =>
    api.post<SQLToRAGResponse>('/rag/ingest-from-sql', request),
  clearVectorDB: () => api.get('/rag/clear'),

  // LLM
  testLLM: (prompt = 'Hello, how are you?') =>
    api.post<{ success: boolean; response: string; error?: string }>('/llm/test', null, {
      params: { prompt },
    }),

  // Credit Control
  creditControlQuery: (question: string) =>
    api.post<CreditControlQueryResponse>('/credit-control/query', { question }),
  creditControlDashboard: () =>
    api.get<CreditControlDashboardResponse>('/credit-control/dashboard'),
  creditControlQueries: () =>
    api.get<{ queries: CreditControlQueryDefinition[] }>('/credit-control/queries'),
  creditControlQueryParam: (queryName: string, params: { account?: string; customer?: string; invoice?: string }) =>
    api.post<CreditControlQueryResponse>('/credit-control/query-param', null, {
      params: { query_name: queryName, ...params },
    }),
  loadCreditControlData: () =>
    api.post('/rag/load-credit-control'),
  debtorsReport: () =>
    api.get<DebtorsReportResponse>('/credit-control/debtors-report'),

  // Nominal Ledger
  trialBalance: (year = 2026) =>
    api.get<TrialBalanceResponse>('/nominal/trial-balance', {
      params: { year },
    }),
  statutoryAccounts: (year = 2026) =>
    api.get<StatutoryAccountsResponse>('/nominal/statutory-accounts', {
      params: { year },
    }),

  // Creditors Control (Purchase Ledger)
  creditorsDashboard: () =>
    api.get<CreditorsDashboardResponse>('/creditors/dashboard'),
  creditorsReport: () =>
    api.get<CreditorsReportResponse>('/creditors/report'),
  supplierDetails: (account: string) =>
    api.get<{ success: boolean; supplier: SupplierDetails }>(`/creditors/supplier/${account}`),
  supplierTransactions: (account: string, includePaid = false) =>
    api.get<SupplierTransactionsResponse>(`/creditors/supplier/${account}/transactions`, {
      params: { include_paid: includePaid },
    }),
  supplierStatement: (account: string, fromDate?: string, toDate?: string) =>
    api.get<SupplierStatementResponse>(`/creditors/supplier/${account}/statement`, {
      params: { from_date: fromDate, to_date: toDate },
    }),
  searchSuppliers: (query: string) =>
    api.get<{ success: boolean; suppliers: SupplierSearchResult[]; count: number }>('/creditors/search', {
      params: { query },
    }),

  // Cashflow Forecast
  cashflowForecast: (yearsHistory = 3) =>
    api.get<CashflowForecastResponse>('/cashflow/forecast', {
      params: { years_history: yearsHistory },
    }),
  cashflowHistory: () =>
    api.get<CashflowHistoryResponse>('/cashflow/history'),

  // Sales Dashboards
  dashboardCeoKpis: (year = 2026) =>
    api.get<DashboardCeoKpisResponse>('/dashboard/ceo-kpis', { params: { year } }),
  dashboardRevenueOverTime: (year = 2026) =>
    api.get<DashboardRevenueOverTimeResponse>('/dashboard/revenue-over-time', { params: { year } }),
  dashboardRevenueComposition: (year = 2026) =>
    api.get<DashboardRevenueCompositionResponse>('/dashboard/revenue-composition', { params: { year } }),
  dashboardTopCustomers: (year = 2026, limit = 20) =>
    api.get<DashboardTopCustomersResponse>('/dashboard/top-customers', { params: { year, limit } }),
  dashboardCustomerConcentration: (year = 2026) =>
    api.get<DashboardCustomerConcentrationResponse>('/dashboard/customer-concentration', { params: { year } }),
  dashboardCustomerLifecycle: (year = 2026) =>
    api.get<DashboardCustomerLifecycleResponse>('/dashboard/customer-lifecycle', { params: { year } }),
  dashboardMarginByCategory: (year = 2026) =>
    api.get<DashboardMarginByCategoryResponse>('/dashboard/margin-by-category', { params: { year } }),

  // Email
  emailProviders: () =>
    api.get<EmailProvidersResponse>('/email/providers'),
  emailAddProvider: (provider: EmailProviderCreate) =>
    api.post<{ success: boolean; provider_id?: number; error?: string }>('/email/providers', provider),
  emailDeleteProvider: (providerId: number) =>
    api.delete<{ success: boolean }>(`/email/providers/${providerId}`),
  emailTestProvider: (providerId: number) =>
    api.post<{ success: boolean; message?: string; error?: string }>(`/email/providers/${providerId}/test`),
  emailFolders: (providerId: number) =>
    api.get<EmailFoldersResponse>(`/email/providers/${providerId}/folders`),
  emailMessages: (params: EmailListParams) =>
    api.get<EmailListResponse>('/email/messages', { params }),
  emailDetail: (emailId: number) =>
    api.get<EmailDetailResponse>(`/email/messages/${emailId}`),
  emailSync: (providerId?: number) =>
    api.post<{ success: boolean; result?: Record<string, unknown>; error?: string }>('/email/sync', null, {
      params: providerId ? { provider_id: providerId } : {},
    }),
  emailSyncStatus: () =>
    api.get<EmailSyncStatusResponse>('/email/sync/status'),
  emailSyncLog: (providerId?: number, limit = 20) =>
    api.get<{ success: boolean; history: EmailSyncLog[] }>('/email/sync/log', {
      params: { provider_id: providerId, limit },
    }),
  emailUpdateCategory: (emailId: number, category: string, reason?: string) =>
    api.put<{ success: boolean }>(`/email/messages/${emailId}/category`, { category, reason }),
  emailLinkCustomer: (emailId: number, accountCode: string) =>
    api.post<{ success: boolean }>(`/email/messages/${emailId}/link`, { account_code: accountCode }),
  emailUnlinkCustomer: (emailId: number) =>
    api.delete<{ success: boolean }>(`/email/messages/${emailId}/link`),
  emailByCustomer: (accountCode: string) =>
    api.get<{ success: boolean; emails: Email[]; count: number }>(`/email/by-customer/${accountCode}`),
  emailStats: () =>
    api.get<EmailStatsResponse>('/email/stats'),
  emailCategorize: (emailId: number) =>
    api.post<{ success: boolean; categorization?: { category: string; confidence: number; reason: string } }>(`/email/messages/${emailId}/categorize`),

  // Companies
  getCompanies: () =>
    api.get<CompaniesResponse>('/companies'),
  getCurrentCompany: () =>
    api.get<CurrentCompanyResponse>('/companies/current'),
  switchCompany: (companyId: string) =>
    api.post<SwitchCompanyResponse>(`/companies/switch/${companyId}`),
  getCompanyConfig: (companyId: string) =>
    api.get<{ company: Company }>(`/companies/${companyId}`),

  // Dashboard - Year Detection
  dashboardAvailableYears: () =>
    api.get<AvailableYearsResponse>('/dashboard/available-years'),
  dashboardSalesCategories: () =>
    api.get<SalesCategoriesResponse>('/dashboard/sales-categories'),

  // Finance Dashboard
  dashboardFinanceSummary: (year: number) =>
    api.get<FinanceSummaryResponse>('/dashboard/finance-summary', { params: { year } }),
  dashboardFinanceMonthly: (year: number) =>
    api.get<FinanceMonthlyResponse>('/dashboard/finance-monthly', { params: { year } }),
  dashboardSalesByProduct: (year: number) =>
    api.get<SalesByProductResponse>('/dashboard/sales-by-product', { params: { year } }),
};

export default apiClient;

// Email Types
export interface EmailProvider {
  id: number;
  name: string;
  provider_type: 'microsoft' | 'gmail' | 'imap';
  enabled: boolean;
  last_sync: string | null;
  sync_status: string;
}

export interface EmailProviderCreate {
  name: string;
  provider_type: 'microsoft' | 'gmail' | 'imap';
  server?: string;
  port?: number;
  username?: string;
  password?: string;
  use_ssl?: boolean;
  tenant_id?: string;
  client_id?: string;
  client_secret?: string;
  credentials_json?: string;
  user_email?: string;
}

export interface EmailProvidersResponse {
  success: boolean;
  providers: EmailProvider[];
}

export interface EmailFolder {
  folder_id: string;
  name: string;
  unread_count: number;
  total_count: number;
  monitored: boolean;
  db_id: number | null;
}

export interface EmailFoldersResponse {
  success: boolean;
  folders: EmailFolder[];
}

export interface Email {
  id: number;
  message_id: string;
  provider_name: string;
  from_address: string;
  from_name: string | null;
  to_addresses: string[];
  subject: string;
  body_preview: string;
  body_text?: string;
  body_html?: string;
  received_at: string;
  is_read: boolean;
  is_flagged: boolean;
  has_attachments: boolean;
  category: string | null;
  category_confidence: number | null;
  linked_account: string | null;
  linked_customer_name?: string | null;
}

export interface EmailListParams {
  provider_id?: number;
  folder_id?: number;
  category?: string;
  linked_account?: string;
  is_read?: boolean;
  from_date?: string;
  to_date?: string;
  search?: string;
  page?: number;
  page_size?: number;
}

export interface EmailListResponse {
  success: boolean;
  emails: Email[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export interface EmailDetailResponse {
  success: boolean;
  email: Email & {
    cc_addresses?: string[];
    attachments?: EmailAttachment[];
    raw_headers?: Record<string, string>;
  };
}

export interface EmailAttachment {
  id: number;
  attachment_id: string;
  filename: string;
  content_type: string;
  size_bytes: number;
}

export interface EmailSyncStatusResponse {
  success: boolean;
  running: boolean;
  providers: {
    id: number;
    name: string;
    type: string;
    enabled: boolean;
    last_sync: string | null;
    sync_status: string;
    registered: boolean;
  }[];
}

export interface EmailSyncLog {
  id: number;
  provider_id: number;
  started_at: string;
  completed_at: string | null;
  status: 'running' | 'success' | 'failed';
  emails_synced: number;
  error_message: string | null;
}

export interface EmailStatsResponse {
  success: boolean;
  stats: {
    total_emails: number;
    unread_count: number;
    linked_count: number;
    categorized_count: number;
  };
  categories: Record<string, number>;
}

// Company types
export interface CompanySettings {
  currency: string;
  currency_symbol: string;
  date_format: string;
  financial_year_start_month: number;
}

export interface CompanyDashboardConfig {
  default_year: number;
  show_margin_analysis: boolean;
  show_customer_lifecycle: boolean;
  revenue_categories_field: string;
  margin_categories_field: string;
}

export interface CompanyModules {
  debtors_control: boolean;
  creditors_control: boolean;
  sales_dashboards: boolean;
  trial_balance: boolean;
  email_integration: boolean;
}

export interface Company {
  id: string;
  name: string;
  database: string;
  description: string;
  settings: CompanySettings;
  dashboard_config: CompanyDashboardConfig;
  modules: CompanyModules;
}

export interface CompaniesResponse {
  companies: Company[];
  current_company: Company | null;
}

export interface CurrentCompanyResponse {
  company: Company | null;
}

export interface SwitchCompanyResponse {
  success: boolean;
  message: string;
  company: Company;
}

// Dashboard types
export interface AvailableYearsResponse {
  success: boolean;
  years: { year: number; transaction_count: number; revenue: number }[];
  default_year: number;
  current_company: string | null;
}

export interface SalesCategoriesResponse {
  success: boolean;
  source: string;
  categories: { category: string; line_count: number; total_value: number }[];
}

export interface FinanceSummaryResponse {
  success: boolean;
  year: number;
  profit_and_loss: {
    sales: number;
    cost_of_sales: number;
    gross_profit: number;
    other_income: number;
    overheads: number;
    operating_profit: number;
  };
  balance_sheet: {
    fixed_assets: number;
    current_assets: number;
    current_liabilities: number;
    net_current_assets: number;
    total_assets: number;
  };
  ratios: {
    gross_margin_percent: number;
    operating_margin_percent: number;
    current_ratio: number;
  };
}

export interface FinanceMonthlyResponse {
  success: boolean;
  year: number;
  months: {
    month: number;
    month_name: string;
    revenue: number;
    cost_of_sales: number;
    gross_profit: number;
    overheads: number;
    net_profit: number;
    gross_margin_percent: number;
  }[];
  ytd: {
    revenue: number;
    cost_of_sales: number;
    gross_profit: number;
    overheads: number;
    net_profit: number;
  };
}

export interface SalesByProductResponse {
  success: boolean;
  year: number;
  categories: {
    category: string;
    category_code: string;
    invoice_count: number;
    line_count: number;
    value: number;
    percent_of_total: number;
  }[];
  total_value: number;
}
