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
};

export default apiClient;
