import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Save, RefreshCw, CheckCircle, AlertCircle, ExternalLink } from 'lucide-react';
import apiClient from '../api/client';
import type { ProviderConfig, DatabaseConfig } from '../api/client';

export function Settings() {
  const queryClient = useQueryClient();

  // LLM Settings State
  const [provider, setProvider] = useState('local');
  const [apiKey, setApiKey] = useState('');
  const [model, setModel] = useState('');
  const [temperature, setTemperature] = useState(0.2);
  const [maxTokens, setMaxTokens] = useState(1000);
  const [ollamaUrl, setOllamaUrl] = useState('http://localhost:11434/api');

  // Database Settings State
  const [dbType, setDbType] = useState('sqlite');
  const [dbServer, setDbServer] = useState('');
  const [dbPort, setDbPort] = useState<number | undefined>(undefined);
  const [dbDatabase, setDbDatabase] = useState('');
  const [dbUsername, setDbUsername] = useState('');
  const [dbPassword, setDbPassword] = useState('');
  const [useWindowsAuth, setUseWindowsAuth] = useState(false);

  // Advanced DB Settings
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [poolSize, setPoolSize] = useState(5);
  const [maxOverflow, setMaxOverflow] = useState(10);
  const [poolTimeout, setPoolTimeout] = useState(30);
  const [connectionTimeout, setConnectionTimeout] = useState(30);
  const [commandTimeout, setCommandTimeout] = useState(60);
  const [useSsl, setUseSsl] = useState(false);

  // Queries
  const { data: providers } = useQuery({
    queryKey: ['providers'],
    queryFn: () => apiClient.getProviders(),
  });

  const { data: models, refetch: refetchModels } = useQuery({
    queryKey: ['models', provider],
    queryFn: () => apiClient.getModels(provider),
    enabled: !!provider,
  });

  const { data: config } = useQuery({
    queryKey: ['config'],
    queryFn: () => apiClient.getConfig(),
  });

  // Load config into state
  useEffect(() => {
    if (config?.data) {
      const cfg = config.data;
      if (cfg.models?.provider) setProvider(cfg.models.provider);
      if (cfg.models?.llm_api_url) setOllamaUrl(cfg.models.llm_api_url);
      if (cfg.system?.temperature) setTemperature(parseFloat(cfg.system.temperature));
      if (cfg.system?.max_token_limit) setMaxTokens(parseInt(cfg.system.max_token_limit));
      if (cfg.database?.type) setDbType(cfg.database.type);
      if (cfg.database?.server) setDbServer(cfg.database.server);
      if (cfg.database?.port) setDbPort(parseInt(cfg.database.port));
      if (cfg.database?.database) setDbDatabase(cfg.database.database);
      if (cfg.database?.username) setDbUsername(cfg.database.username);
      if (cfg.database?.use_windows_auth) setUseWindowsAuth(cfg.database.use_windows_auth === 'True');
      if (cfg.database?.pool_size) setPoolSize(parseInt(cfg.database.pool_size));
      if (cfg.database?.max_overflow) setMaxOverflow(parseInt(cfg.database.max_overflow));
      if (cfg.database?.pool_timeout) setPoolTimeout(parseInt(cfg.database.pool_timeout));
      if (cfg.database?.connection_timeout) setConnectionTimeout(parseInt(cfg.database.connection_timeout));
      if (cfg.database?.command_timeout) setCommandTimeout(parseInt(cfg.database.command_timeout));
      if (cfg.database?.ssl) setUseSsl(cfg.database.ssl === 'true');

      // Set model based on provider
      if (cfg.models?.provider === 'local' && cfg.models?.llm_model) {
        setModel(cfg.models.llm_model);
      } else if (cfg[cfg.models?.provider]?.model) {
        setModel(cfg[cfg.models.provider].model);
      }
    }
  }, [config]);

  // Set default model when models list loads
  useEffect(() => {
    if (models?.data?.models && models.data.models.length > 0 && !model) {
      setModel(models.data.models[0]);
    }
  }, [models, model]);

  // Mutations
  const llmMutation = useMutation({
    mutationFn: (data: ProviderConfig) => apiClient.updateLLMConfig(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['config'] });
      queryClient.invalidateQueries({ queryKey: ['status'] });
    },
  });

  const dbMutation = useMutation({
    mutationFn: (data: DatabaseConfig) => apiClient.updateDatabaseConfig(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['config'] });
      queryClient.invalidateQueries({ queryKey: ['status'] });
    },
  });

  const testMutation = useMutation({
    mutationFn: () => apiClient.testLLM(),
  });

  const handleSaveLLM = () => {
    llmMutation.mutate({
      provider,
      api_key: apiKey || undefined,
      model,
      temperature,
      max_tokens: maxTokens,
      ollama_url: provider === 'local' ? ollamaUrl : undefined,
    });
  };

  const handleSaveDatabase = () => {
    dbMutation.mutate({
      type: dbType,
      server: dbServer || undefined,
      port: dbPort,
      database: dbDatabase || undefined,
      username: dbUsername || undefined,
      password: dbPassword || undefined,
      use_windows_auth: useWindowsAuth,
      pool_size: poolSize,
      max_overflow: maxOverflow,
      pool_timeout: poolTimeout,
      connection_timeout: connectionTimeout,
      command_timeout: commandTimeout,
      ssl: useSsl,
    });
  };

  const providerInfo: Record<string, { name: string; link: string; linkText: string }> = {
    local: { name: 'Ollama (Local)', link: 'https://ollama.ai', linkText: 'Install Ollama' },
    openai: { name: 'OpenAI', link: 'https://platform.openai.com/api-keys', linkText: 'Get API Key' },
    anthropic: { name: 'Anthropic Claude', link: 'https://console.anthropic.com/settings/keys', linkText: 'Get API Key' },
    gemini: { name: 'Google Gemini', link: 'https://aistudio.google.com/apikey', linkText: 'Get API Key' },
    groq: { name: 'Groq', link: 'https://console.groq.com/keys', linkText: 'Get API Key' },
  };

  const currentProvider = providerInfo[provider] || providerInfo.local;
  const requiresApiKey = provider !== 'local';

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold text-gray-900">Settings</h2>
        <p className="text-gray-600 mt-1">Configure your LLM provider and database connection</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* LLM Settings */}
        <div className="card">
          <h3 className="text-lg font-semibold mb-4">LLM Configuration</h3>

          <div className="space-y-4">
            {/* Provider Selection */}
            <div>
              <label className="label">Provider</label>
              <select
                className="select"
                value={provider}
                onChange={(e) => {
                  setProvider(e.target.value);
                  setModel('');
                  setApiKey('');
                }}
              >
                {providers?.data?.providers.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name}
                  </option>
                ))}
              </select>
              <a
                href={currentProvider.link}
                target="_blank"
                rel="noopener noreferrer"
                className="text-sm text-blue-600 hover:text-blue-800 flex items-center mt-1"
              >
                {currentProvider.linkText}
                <ExternalLink className="h-3 w-3 ml-1" />
              </a>
            </div>

            {/* API Key (for non-local providers) */}
            {requiresApiKey && (
              <div>
                <label className="label">API Key</label>
                <input
                  type="password"
                  className="input"
                  placeholder="Enter your API key"
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                />
              </div>
            )}

            {/* Ollama URL (for local provider) */}
            {provider === 'local' && (
              <div>
                <label className="label">Ollama API URL</label>
                <input
                  type="text"
                  className="input"
                  placeholder="http://localhost:11434/api"
                  value={ollamaUrl}
                  onChange={(e) => setOllamaUrl(e.target.value)}
                />
                <p className="text-xs text-gray-500 mt-1">
                  Use a network address (e.g., http://192.168.1.100:11434/api) if Ollama runs on another Mac
                </p>
              </div>
            )}

            {/* Model Selection */}
            <div>
              <label className="label">Model</label>
              <select
                className="select"
                value={model}
                onChange={(e) => setModel(e.target.value)}
              >
                {models?.data?.models.map((m) => (
                  <option key={m} value={m}>
                    {m}
                  </option>
                ))}
              </select>
              <button
                onClick={() => refetchModels()}
                className="text-sm text-blue-600 hover:text-blue-800 flex items-center mt-1"
              >
                <RefreshCw className="h-3 w-3 mr-1" />
                Refresh models
              </button>
            </div>

            {/* Temperature */}
            <div>
              <label className="label">Temperature: {temperature}</label>
              <input
                type="range"
                min="0"
                max="1"
                step="0.1"
                value={temperature}
                onChange={(e) => setTemperature(parseFloat(e.target.value))}
                className="w-full"
              />
              <div className="flex justify-between text-xs text-gray-500">
                <span>Precise</span>
                <span>Creative</span>
              </div>
            </div>

            {/* Max Tokens */}
            <div>
              <label className="label">Max Tokens</label>
              <input
                type="number"
                className="input"
                min="100"
                max="8000"
                step="100"
                value={maxTokens}
                onChange={(e) => setMaxTokens(parseInt(e.target.value))}
              />
            </div>

            {/* Actions */}
            <div className="flex space-x-3 pt-2">
              <button
                onClick={handleSaveLLM}
                disabled={llmMutation.isPending}
                className="btn btn-primary flex items-center"
              >
                <Save className="h-4 w-4 mr-2" />
                {llmMutation.isPending ? 'Saving...' : 'Save LLM Config'}
              </button>
              <button
                onClick={() => testMutation.mutate()}
                disabled={testMutation.isPending}
                className="btn btn-secondary flex items-center"
              >
                {testMutation.isPending ? 'Testing...' : 'Test Connection'}
              </button>
            </div>

            {/* Status Messages */}
            {llmMutation.isSuccess && (
              <div className="flex items-center text-green-600 text-sm">
                <CheckCircle className="h-4 w-4 mr-1" />
                LLM configuration saved successfully
              </div>
            )}
            {llmMutation.isError && (
              <div className="flex items-center text-red-600 text-sm">
                <AlertCircle className="h-4 w-4 mr-1" />
                Failed to save configuration
              </div>
            )}
            {testMutation.isSuccess && (
              <div className="bg-gray-100 p-3 rounded-md text-sm">
                <strong>Response:</strong> {testMutation.data?.data?.response?.substring(0, 200)}...
              </div>
            )}
            {testMutation.isError && (
              <div className="flex items-center text-red-600 text-sm">
                <AlertCircle className="h-4 w-4 mr-1" />
                LLM test failed
              </div>
            )}
          </div>
        </div>

        {/* Database Settings */}
        <div className="card">
          <h3 className="text-lg font-semibold mb-4">Database Configuration</h3>

          <div className="space-y-4">
            {/* Database Type */}
            <div>
              <label className="label">Database Type</label>
              <select
                className="select"
                value={dbType}
                onChange={(e) => setDbType(e.target.value)}
              >
                <option value="sqlite">SQLite</option>
                <option value="mssql">Microsoft SQL Server</option>
                <option value="postgresql">PostgreSQL</option>
                <option value="mysql">MySQL</option>
              </select>
            </div>

            {dbType !== 'sqlite' && (
              <>
                {/* Server & Port */}
                <div className="grid grid-cols-3 gap-3">
                  <div className="col-span-2">
                    <label className="label">Server</label>
                    <input
                      type="text"
                      className="input"
                      placeholder="localhost or IP address"
                      value={dbServer}
                      onChange={(e) => setDbServer(e.target.value)}
                    />
                  </div>
                  <div>
                    <label className="label">Port</label>
                    <input
                      type="number"
                      className="input"
                      placeholder={dbType === 'mssql' ? '1433' : dbType === 'postgresql' ? '5432' : '3306'}
                      value={dbPort || ''}
                      onChange={(e) => setDbPort(e.target.value ? parseInt(e.target.value) : undefined)}
                    />
                  </div>
                </div>

                {/* Database Name */}
                <div>
                  <label className="label">Database Name</label>
                  <input
                    type="text"
                    className="input"
                    placeholder="Database name"
                    value={dbDatabase}
                    onChange={(e) => setDbDatabase(e.target.value)}
                  />
                </div>

                {/* Windows Authentication (MS SQL only) */}
                {dbType === 'mssql' && (
                  <div className="flex items-center">
                    <input
                      type="checkbox"
                      id="windowsAuth"
                      checked={useWindowsAuth}
                      onChange={(e) => setUseWindowsAuth(e.target.checked)}
                      className="mr-2"
                    />
                    <label htmlFor="windowsAuth" className="text-sm text-gray-700">
                      Use Windows Authentication
                    </label>
                  </div>
                )}

                {/* Username & Password (when not using Windows Auth) */}
                {!useWindowsAuth && (
                  <>
                    <div>
                      <label className="label">Username</label>
                      <input
                        type="text"
                        className="input"
                        placeholder="Database username"
                        value={dbUsername}
                        onChange={(e) => setDbUsername(e.target.value)}
                      />
                    </div>
                    <div>
                      <label className="label">Password</label>
                      <input
                        type="password"
                        className="input"
                        placeholder="Database password"
                        value={dbPassword}
                        onChange={(e) => setDbPassword(e.target.value)}
                      />
                    </div>
                  </>
                )}

                {/* Advanced Settings Toggle */}
                <button
                  type="button"
                  onClick={() => setShowAdvanced(!showAdvanced)}
                  className="text-sm text-blue-600 hover:text-blue-800 flex items-center"
                >
                  {showAdvanced ? '▼' : '▶'} Advanced Connection Settings
                </button>

                {/* Advanced Settings Panel */}
                {showAdvanced && (
                  <div className="bg-gray-50 p-4 rounded-md space-y-3">
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <label className="label text-xs">Pool Size</label>
                        <input
                          type="number"
                          className="input text-sm"
                          value={poolSize}
                          onChange={(e) => setPoolSize(parseInt(e.target.value))}
                        />
                      </div>
                      <div>
                        <label className="label text-xs">Max Overflow</label>
                        <input
                          type="number"
                          className="input text-sm"
                          value={maxOverflow}
                          onChange={(e) => setMaxOverflow(parseInt(e.target.value))}
                        />
                      </div>
                    </div>
                    <div className="grid grid-cols-3 gap-3">
                      <div>
                        <label className="label text-xs">Pool Timeout (s)</label>
                        <input
                          type="number"
                          className="input text-sm"
                          value={poolTimeout}
                          onChange={(e) => setPoolTimeout(parseInt(e.target.value))}
                        />
                      </div>
                      <div>
                        <label className="label text-xs">Connect Timeout (s)</label>
                        <input
                          type="number"
                          className="input text-sm"
                          value={connectionTimeout}
                          onChange={(e) => setConnectionTimeout(parseInt(e.target.value))}
                        />
                      </div>
                      <div>
                        <label className="label text-xs">Command Timeout (s)</label>
                        <input
                          type="number"
                          className="input text-sm"
                          value={commandTimeout}
                          onChange={(e) => setCommandTimeout(parseInt(e.target.value))}
                        />
                      </div>
                    </div>
                    <div className="flex items-center">
                      <input
                        type="checkbox"
                        id="useSsl"
                        checked={useSsl}
                        onChange={(e) => setUseSsl(e.target.checked)}
                        className="mr-2"
                      />
                      <label htmlFor="useSsl" className="text-sm text-gray-700">
                        Use SSL/TLS Connection
                      </label>
                    </div>
                  </div>
                )}
              </>
            )}

            {dbType === 'sqlite' && (
              <div className="bg-blue-50 p-3 rounded-md text-sm text-blue-800">
                SQLite mode uses a local file database. Configure the path in the config.ini file.
              </div>
            )}

            {/* Save Button */}
            <div className="pt-2">
              <button
                onClick={handleSaveDatabase}
                disabled={dbMutation.isPending}
                className="btn btn-primary flex items-center"
              >
                <Save className="h-4 w-4 mr-2" />
                {dbMutation.isPending ? 'Saving...' : 'Save Database Config'}
              </button>
            </div>

            {/* Status Messages */}
            {dbMutation.isSuccess && (
              <div className="flex items-center text-green-600 text-sm">
                <CheckCircle className="h-4 w-4 mr-1" />
                Database configuration saved successfully
              </div>
            )}
            {dbMutation.isError && (
              <div className="flex items-center text-red-600 text-sm">
                <AlertCircle className="h-4 w-4 mr-1" />
                Failed to save database configuration
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
