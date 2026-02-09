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

export interface OperaConfig {
  version: 'sql_se' | 'opera3';
  opera3_server_path?: string;
  opera3_base_path?: string;
  opera3_company_code?: string;
}

export interface Opera3Company {
  code: string;
  name: string;
  data_path: string;
}

export interface Opera3CompaniesResponse {
  companies: Opera3Company[];
  error?: string;
}

export interface OperaTestResponse {
  success: boolean;
  message?: string;
  error?: string;
  companies_count?: number;
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

// Reconciliation Types
export interface ReconciliationTransactionType {
  type: string;
  description: string;
  count: number;
  total: number;
}

export interface ReconciliationControlAccount {
  account: string;
  description: string;
  brought_forward: number;
  current_year?: number;
  current_year_debits?: number;
  current_year_credits?: number;
  current_year_net?: number;
  ytd_debits?: number;
  ytd_credits?: number;
  ytd_movement?: number;
  closing_balance: number;
  ntran_by_year?: { year: number; debits: number; credits: number; net: number }[];
}

export interface ReconciliationAgedBand {
  age_band: string;
  count: number;
  total: number;
}

export interface ReconciliationTopEntity {
  account: string;
  name: string;
  invoice_count: number;
  outstanding: number;
}

export interface ReconciliationPendingTransaction {
  account: string;
  supplier?: string;
  customer?: string;
  type: string;
  type_desc: string;
  reference: string;
  date: string;
  value: number;
  balance: number;
  detail: string;
}

export interface ReconciliationPostingStatus {
  count: number;
  total: number;
  transactions?: ReconciliationPendingTransaction[];
}

export interface ReconciliationResponse {
  success: boolean;
  reconciliation_date: string;
  purchase_ledger?: {
    source: string;
    total_outstanding: number;
    transaction_count: number;
    breakdown_by_type: ReconciliationTransactionType[];
    posted_to_nl?: ReconciliationPostingStatus;
    pending_transfer?: ReconciliationPostingStatus;
    supplier_master_check: {
      source: string;
      total: number;
      supplier_count: number;
      matches_ptran: boolean;
    };
  };
  sales_ledger?: {
    source: string;
    total_outstanding: number;
    transaction_count: number;
    breakdown_by_type: ReconciliationTransactionType[];
    posted_to_nl?: ReconciliationPostingStatus;
    pending_transfer?: ReconciliationPostingStatus;
    customer_master_check: {
      source: string;
      total: number;
      customer_count: number;
      matches_stran: boolean;
    };
  };
  nominal_ledger: {
    source: string;
    control_accounts: ReconciliationControlAccount[];
    total_balance: number;
    current_year?: number;
  };
  variance: {
    amount: number;
    absolute: number;
    purchase_ledger_total?: number;
    purchase_ledger_posted?: number;
    purchase_ledger_pending?: number;
    sales_ledger_total?: number;
    sales_ledger_posted?: number;
    sales_ledger_pending?: number;
    nominal_ledger_total: number;
    posted_variance?: number;
    posted_variance_abs?: number;
    reconciled: boolean;
    has_pending_transfers?: boolean;
  };
  status: string;
  message: string;
  aged_analysis?: ReconciliationAgedBand[];
  top_suppliers?: ReconciliationTopEntity[];
  error?: string;
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

// Supplier Statement Automation Types
export interface SupplierStatementDashboardKpis {
  statements_today: number;
  statements_week: number;
  statements_month: number;
  pending_approvals: number;
  open_queries: number;
  overdue_queries: number;
  avg_processing_hours: number | null;
  match_rate_percent: number | null;
}

export interface SupplierStatementDashboardAlert {
  id: number;
  supplier_code: string;
  supplier_name: string;
  alert_type: string;
  message: string;
  created_at: string;
}

export interface SupplierStatementSummary {
  id: number;
  supplier_code: string;
  supplier_name: string;
  statement_date: string | null;
  received_date: string;
  status: string;
  closing_balance: number | null;
}

export interface SupplierQuerySummary {
  id: number;
  supplier_code: string;
  supplier_name: string;
  query_type: string;
  reference: string | null;
  status: string;
  days_outstanding: number;
  created_at: string;
}

export interface SupplierResponseSummary {
  id: number;
  supplier_code: string;
  supplier_name: string;
  statement_date: string | null;
  sent_at: string;
  approved_by: string | null;
  queries_count: number;
  balance: number | null;
}

export interface SupplierStatementDashboardResponse {
  success: boolean;
  kpis: SupplierStatementDashboardKpis;
  alerts: {
    security_alerts: SupplierStatementDashboardAlert[];
    overdue_queries: SupplierStatementDashboardAlert[];
    failed_processing: SupplierStatementDashboardAlert[];
  };
  recent_statements: SupplierStatementSummary[];
  recent_queries: SupplierQuerySummary[];
  recent_responses: SupplierResponseSummary[];
  error?: string;
}

export interface SupplierStatementQueueItem {
  id: number;
  supplier_code: string;
  supplier_name: string;
  statement_date: string | null;
  received_date: string;
  sender_email: string | null;
  status: string;
  opening_balance: number | null;
  closing_balance: number | null;
  currency: string;
  acknowledged_at: string | null;
  processed_at: string | null;
  approved_by: string | null;
  approved_at: string | null;
  sent_at: string | null;
  error_message: string | null;
  line_count: number;
  matched_count: number;
  query_count: number;
}

export interface SupplierStatementQueueResponse {
  success: boolean;
  statements: SupplierStatementQueueItem[];
  error?: string;
}

export interface SupplierStatementLineItem {
  id: number;
  line_date: string | null;
  reference: string | null;
  description: string | null;
  debit: number | null;
  credit: number | null;
  balance: number | null;
  doc_type: string | null;
  match_status: string;
  matched_ptran_id: string | null;
  query_type: string | null;
  query_sent_at: string | null;
  query_resolved_at: string | null;
}

export interface SupplierStatementDetailResponse {
  success: boolean;
  statement: SupplierStatementQueueItem;
  error?: string;
}

export interface SupplierStatementLinesResponse {
  success: boolean;
  lines: SupplierStatementLineItem[];
  summary: {
    total_lines: number;
    total_debits: number;
    total_credits: number;
    matched_count: number;
    query_count: number;
    unmatched_count: number;
  };
  error?: string;
}

export interface SupplierStatementExtractResponse {
  success: boolean;
  source: string;
  filename?: string;
  email_subject?: string;
  from_address?: string;
  statement_info: {
    supplier_name: string;
    account_reference: string | null;
    statement_date: string | null;
    opening_balance: number | null;
    closing_balance: number | null;
    currency: string;
  };
  lines: Array<{
    date: string;
    reference: string | null;
    description: string | null;
    debit: number | null;
    credit: number | null;
    balance: number | null;
    doc_type: string | null;
  }>;
  summary: {
    total_lines: number;
    total_debits: number;
    total_credits: number;
  };
  error?: string;
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

  // Opera Configuration
  getOperaConfig: () => api.get<OperaConfig>('/config/opera'),
  updateOperaConfig: (config: OperaConfig) => api.post('/config/opera', config),
  getOpera3Companies: () => api.get<Opera3CompaniesResponse>('/config/opera/companies'),
  testOperaConnection: (config: OperaConfig) => api.post<OperaTestResponse>('/config/opera/test', config),

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

  // Supplier Statement Automation
  supplierStatementDashboard: () =>
    api.get<SupplierStatementDashboardResponse>('/supplier-statements/dashboard'),
  supplierStatementQueue: (status?: string) =>
    api.get<SupplierStatementQueueResponse>('/supplier-statements', {
      params: status ? { status } : {},
    }),
  supplierStatementDetail: (statementId: number) =>
    api.get<SupplierStatementDetailResponse>(`/supplier-statements/${statementId}`),
  supplierStatementLines: (statementId: number) =>
    api.get<SupplierStatementLinesResponse>(`/supplier-statements/${statementId}/lines`),
  supplierStatementReconciliations: () =>
    api.get<SupplierStatementQueueResponse>('/supplier-statements/reconciliations'),
  supplierStatementApprove: (statementId: number, approvedBy?: string) =>
    api.post<{ success: boolean; message?: string; error?: string }>(`/supplier-statements/${statementId}/approve`, null, {
      params: approvedBy ? { approved_by: approvedBy } : {},
    }),
  supplierStatementProcess: (statementId: number) =>
    api.post<{ success: boolean; message?: string; error?: string }>(`/supplier-statements/${statementId}/process`),
  supplierStatementExtractFromEmail: (emailId: number, attachmentId?: string) =>
    api.post<SupplierStatementExtractResponse>(`/supplier-statements/extract-from-email/${emailId}`, null, {
      params: attachmentId ? { attachment_id: attachmentId } : {},
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
  emailGetProvider: (providerId: number) =>
    api.get<{ success: boolean; provider?: { id: number; name: string; provider_type: string; enabled: number; config: Record<string, unknown> }; error?: string }>(`/email/providers/${providerId}`),
  emailUpdateProvider: (providerId: number, provider: EmailProviderCreate) =>
    api.put<{ success: boolean; message?: string; error?: string }>(`/email/providers/${providerId}`, provider),
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

  // Enhanced Sales Dashboard (Intsys UK)
  dashboardExecutiveSummary: (year: number) =>
    api.get<ExecutiveSummaryResponse>('/dashboard/executive-summary', { params: { year } }),
  dashboardRevenueByCategoryDetailed: (year: number) =>
    api.get<RevenueByCategoryDetailedResponse>('/dashboard/revenue-by-category-detailed', { params: { year } }),
  dashboardNewVsExistingRevenue: (year: number) =>
    api.get<NewVsExistingRevenueResponse>('/dashboard/new-vs-existing-revenue', { params: { year } }),
  dashboardCustomerChurnAnalysis: (year: number) =>
    api.get<CustomerChurnAnalysisResponse>('/dashboard/customer-churn-analysis', { params: { year } }),
  dashboardForwardIndicators: (year: number) =>
    api.get<ForwardIndicatorsResponse>('/dashboard/forward-indicators', { params: { year } }),
  dashboardMonthlyComparison: (year: number) =>
    api.get<MonthlyComparisonResponse>('/dashboard/monthly-comparison', { params: { year } }),

  // Reconciliation
  reconcileCreditors: () =>
    api.get<ReconciliationResponse>('/reconcile/creditors'),
  reconcileDebtors: () =>
    api.get<ReconciliationResponse>('/reconcile/debtors'),
  reconcileVat: () =>
    api.get<any>('/reconcile/vat'),
  reconcileBanks: () =>
    api.get<BankAccountsResponse>('/reconcile/banks'),
  reconcileBank: (bankCode: string) =>
    api.get<BankReconciliationResponse>(`/reconcile/bank/${bankCode}`),

  // Bank Statement Reconciliation (mark entries as reconciled)
  getBankReconciliationStatus: (bankCode: string) =>
    api.get<BankReconciliationStatusResponse>(`/reconcile/bank/${bankCode}/status`),
  getUnreconciledEntries: (bankCode: string) =>
    api.get<UnreconciledEntriesResponse>(`/reconcile/bank/${bankCode}/unreconciled`),
  markEntriesReconciled: (bankCode: string, data: MarkReconciledRequest) =>
    api.post<MarkReconciledResponse>(`/reconcile/bank/${bankCode}/mark-reconciled`, data),
  unreconcileEntries: (bankCode: string, entryNumbers: string[]) =>
    api.post<UnreconcileResponse>(`/reconcile/bank/${bankCode}/unreconcile`, entryNumbers),
};

// Bank Reconciliation Types
export interface BankAccountInfo {
  account_code: string;
  description: string;
  sort_code: string;
  account_number: string;
}

export interface BankAccountsResponse {
  success: boolean;
  banks: BankAccountInfo[];
  error?: string;
}

export interface BankReconciliationResponse {
  success: boolean;
  reconciliation_date: string;
  bank_code: string;
  bank_account: {
    code: string;
    description: string;
    sort_code: string;
    account_number: string;
  };
  cashbook: {
    source: string;
    current_year?: number;
    current_year_entries?: number;
    current_year_transactions?: number;
    current_year_receipts?: number;
    current_year_payments?: number;
    current_year_movements?: number;
    prior_year_bf?: number;
    expected_closing?: number;
    all_time_entries?: number;
    all_time_net?: number;
    // Legacy fields for backwards compatibility
    total_balance?: number;
    entry_count?: number;
    transfer_file: {
      source: string;
      posted_to_nl: {
        count: number;
        total: number;
      };
      pending_transfer: {
        count: number;
        total: number;
        transactions: {
          nominal_account: string;
          source: string;
          source_desc: string;
          date: string;
          value: number;
          reference: string;
          comment: string;
        }[];
      };
    };
  };
  bank_master?: {
    source: string;
    balance_pence: number;
    balance_pounds: number;
  };
  nominal_ledger: {
    source: string;
    account: string;
    description: string;
    current_year?: number;
    brought_forward?: number;
    current_year_debits?: number;
    current_year_credits?: number;
    current_year_net?: number;
    closing_balance?: number;
    total_balance: number;
  };
  variance: {
    // New structured variance format
    cashbook_vs_bank_master?: {
      description: string;
      cashbook_expected: number;
      bank_master: number;
      amount: number;
      absolute: number;
      reconciled: boolean;
    };
    bank_master_vs_nominal?: {
      description: string;
      bank_master: number;
      nominal_ledger: number;
      amount: number;
      absolute: number;
      reconciled: boolean;
    };
    cashbook_vs_nominal?: {
      description: string;
      cashbook_expected: number;
      nominal_ledger: number;
      amount: number;
      absolute: number;
      reconciled: boolean;
    };
    summary?: {
      current_year: number;
      cashbook_movements: number;
      prior_year_bf: number;
      cashbook_expected_closing: number;
      bank_master_balance: number;
      nominal_ledger_balance: number;
      transfer_file_pending: number;
      all_reconciled: boolean;
      has_pending_transfers: boolean;
    };
    // Legacy fields for backwards compatibility
    amount?: number;
    absolute?: number;
    cashbook_total?: number;
    transfer_file_posted?: number;
    transfer_file_pending?: number;
    nominal_ledger_total?: number;
    reconciled?: boolean;
    has_pending_transfers?: boolean;
  };
  status: string;
  message: string;
  error?: string;
}

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

// Enhanced Sales Dashboard Types (Intsys UK)
export interface ExecutiveSummaryResponse {
  success: boolean;
  year: number;
  period: {
    current_month: number;
    current_quarter: number;
    months_elapsed: number;
  };
  kpis: {
    current_month: {
      value: number;
      prior_year: number;
      yoy_change_percent: number;
      trend: 'up' | 'down' | 'flat';
    };
    quarter_to_date: {
      value: number;
      prior_year: number;
      yoy_change_percent: number;
      trend: 'up' | 'down' | 'flat';
    };
    year_to_date: {
      value: number;
      prior_year: number;
      yoy_change_percent: number;
      trend: 'up' | 'down' | 'flat';
    };
    rolling_12_months: {
      value: number;
      prior_period: number;
      change_percent: number;
      trend: 'up' | 'down' | 'flat';
    };
    monthly_run_rate: number;
    annual_run_rate: number;
    projected_full_year: number;
    prior_full_year: number;
    projection_vs_prior_percent: number;
  };
}

export interface RevenueByCategoryDetailedResponse {
  success: boolean;
  year: number;
  summary: {
    total_current: number;
    total_previous: number;
    total_change_percent: number;
  };
  categories: {
    category: string;
    current_year: number;
    previous_year: number;
    change_amount: number;
    change_percent: number;
    percent_of_total: number;
    trend: 'up' | 'down' | 'stable';
    monthly_trend: { month: number; current: number; previous: number }[];
  }[];
}

export interface NewVsExistingRevenueResponse {
  success: boolean;
  year: number;
  summary: {
    total_revenue: number;
    total_customers: number;
  };
  new_business: {
    this_year: {
      customers: number;
      revenue: number;
      percent_of_total: number;
      avg_per_customer: number;
    };
    last_year_acquired: {
      customers: number;
      revenue: number;
      percent_of_total: number;
      avg_per_customer: number;
    };
  };
  existing_business: {
    customers: number;
    revenue: number;
    percent_of_total: number;
    avg_per_customer: number;
  };
}

export interface CustomerChurnAnalysisResponse {
  success: boolean;
  year: number;
  summary: {
    retention_rate: number;
    churned_count: number;
    churned_revenue: number;
    at_risk_count: number;
    at_risk_revenue: number;
    growing_count: number;
    stable_count: number;
    declining_count: number;
  };
  churned_customers: {
    account: string;
    customer_name: string;
    last_year_revenue: number;
    last_invoice: string | null;
  }[];
  at_risk_customers: {
    account: string;
    customer_name: string;
    current_revenue: number;
    previous_revenue: number;
    change_percent: number;
  }[];
  growing_customers: {
    account: string;
    customer_name: string;
    current_revenue: number;
    previous_revenue: number;
    change_percent: number;
  }[];
}

export interface ForwardIndicatorsResponse {
  success: boolean;
  year: number;
  current_month: number;
  run_rates: {
    monthly_3m_avg: number;
    monthly_6m_avg: number;
    monthly_ytd_avg: number;
    annual_3m_basis: number;
    annual_6m_basis: number;
    annual_ytd_basis: number;
  };
  trend: {
    direction: 'accelerating' | 'decelerating' | 'stable';
    recent_3_months: number;
    prior_3_months: number;
  };
  projections: {
    conservative: number;
    optimistic: number;
    midpoint: number;
    prior_year_actual: number;
    vs_prior_year_percent: number;
  };
  risk_flags: {
    type: string;
    severity: 'low' | 'medium' | 'high';
    message: string;
  }[];
  risk_level: 'low' | 'medium' | 'high';
}

export interface MonthlyComparisonResponse {
  success: boolean;
  year: number;
  months: {
    month: number;
    month_name: string;
    current_year: number;
    previous_year: number;
    two_years_ago: number;
    yoy_change_amount: number;
    yoy_change_percent: number;
    gross_profit: number;
    gross_margin_percent: number;
    ytd_current: number;
    ytd_previous: number;
    ytd_variance: number;
  }[];
  totals: {
    current_year: number;
    previous_year: number;
    two_years_ago: number;
  };
}


// Bank Statement Reconciliation Types
export interface BankReconciliationStatusResponse {
  success: boolean;
  bank_account: string;
  reconciled_balance: number;
  current_balance: number;
  unreconciled_difference: number;
  unreconciled_count: number;
  unreconciled_total: number;
  last_rec_line: number;
  last_stmt_no: number | null;
  last_stmt_date: string | null;
  last_rec_date: string | null;
  error?: string;
}

export interface UnreconciledEntry {
  ae_entry: string;
  value_pounds: number;
  ae_lstdate: string | null;
  ae_cbtype: string;
  ae_entref: string;
  ae_comment: string;
}

export interface UnreconciledEntriesResponse {
  success: boolean;
  bank_code: string;
  count: number;
  entries: UnreconciledEntry[];
  error?: string;
}

export interface ReconcileEntryInput {
  entry_number: string;
  statement_line: number;
}

export interface MarkReconciledRequest {
  entries: ReconcileEntryInput[];
  statement_number: number;
  statement_date?: string;
  reconciliation_date?: string;
}

export interface MarkReconciledResponse {
  success: boolean;
  message?: string;
  records_reconciled?: number;
  details?: string[];
  errors?: string[];
}

export interface UnreconcileResponse {
  success: boolean;
  message?: string;
  entries_unreconciled?: number;
  new_reconciled_balance?: number;
  error?: string;
}
