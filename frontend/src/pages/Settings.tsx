import { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Save, RefreshCw, CheckCircle, AlertCircle, ExternalLink, Mail, Trash2, TestTube, Pencil, X, Settings as SettingsIcon } from 'lucide-react';
import apiClient from '../api/client';
import type { ProviderConfig, EmailProviderCreate, EmailProvider } from '../api/client';
import { PageHeader, Card } from '../components/ui';

export function Settings() {
  const queryClient = useQueryClient();

  // LLM Settings State
  const [provider, setProvider] = useState('local');
  const [apiKey, setApiKey] = useState('');
  const [model, setModel] = useState('');
  const [temperature, setTemperature] = useState(0.2);
  const [maxTokens, setMaxTokens] = useState(1000);
  const [ollamaUrl, setOllamaUrl] = useState('http://localhost:11434/api');

  // Email Provider Settings
  const [emailProviderType, setEmailProviderType] = useState<'microsoft' | 'gmail' | 'imap'>('microsoft');
  const [emailProviderName, setEmailProviderName] = useState('');
  // Microsoft/Office 365
  const [tenantId, setTenantId] = useState('');
  const [clientId, setClientId] = useState('');
  const [clientSecret, setClientSecret] = useState('');
  const [userEmail, setUserEmail] = useState('');
  // IMAP
  const [imapServer, setImapServer] = useState('');
  const [imapPort, setImapPort] = useState(993);
  const [imapUsername, setImapUsername] = useState('');
  const [imapPassword, setImapPassword] = useState('');
  const [imapUseSsl, setImapUseSsl] = useState(true);
  // Edit mode
  const [editingProviderId, setEditingProviderId] = useState<number | null>(null);


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

  // Email provider queries
  const { data: emailProviders, refetch: refetchEmailProviders } = useQuery({
    queryKey: ['emailProviders'],
    queryFn: () => apiClient.emailProviders(),
  });

  // Load config into state
  useEffect(() => {
    if (config?.data) {
      const cfg = config.data;
      if (cfg.models?.provider) setProvider(cfg.models.provider);
      if (cfg.models?.llm_api_url) setOllamaUrl(cfg.models.llm_api_url);
      if (cfg.system?.temperature) setTemperature(parseFloat(cfg.system.temperature));
      if (cfg.system?.max_token_limit) setMaxTokens(parseInt(cfg.system.max_token_limit));

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

  const testMutation = useMutation({
    mutationFn: () => apiClient.testLLM(),
  });

  // Email provider mutations
  const addEmailProviderMutation = useMutation({
    mutationFn: (data: EmailProviderCreate) => apiClient.emailAddProvider(data),
    onSuccess: (response) => {
      console.log('Email provider response:', response.data);
      if (response.data?.success) {
        refetchEmailProviders();
        // Reset form only on success
        setEmailProviderName('');
        setTenantId('');
        setClientId('');
        setClientSecret('');
        setUserEmail('');
        setImapServer('');
        setImapUsername('');
        setImapPassword('');
      }
    },
    onError: (error) => {
      console.error('Email provider error:', error);
    },
  });

  const deleteEmailProviderMutation = useMutation({
    mutationFn: (providerId: number) => apiClient.emailDeleteProvider(providerId),
    onSuccess: () => {
      refetchEmailProviders();
    },
  });

  const testEmailProviderMutation = useMutation({
    mutationFn: (providerId: number) => apiClient.emailTestProvider(providerId),
  });

  const updateEmailProviderMutation = useMutation({
    mutationFn: ({ providerId, data }: { providerId: number; data: EmailProviderCreate }) =>
      apiClient.emailUpdateProvider(providerId, data),
    onSuccess: (response) => {
      if (response.data?.success) {
        refetchEmailProviders();
        // Reset form and exit edit mode
        setEditingProviderId(null);
        setEmailProviderName('');
        setImapServer('');
        setImapUsername('');
        setImapPassword('');
      }
    },
  });

  const handleEditProvider = async (provider: EmailProvider) => {
    setEditingProviderId(provider.id);
    setEmailProviderName(provider.name);
    setEmailProviderType(provider.provider_type as 'microsoft' | 'gmail' | 'imap');

    // Fetch full provider config to populate fields (except passwords for security)
    try {
      const response = await apiClient.emailGetProvider(provider.id);
      if (response.data?.success && response.data.provider?.config) {
        const config = response.data.provider.config;
        if (provider.provider_type === 'imap') {
          setImapServer((config.server as string) || '');
          setImapPort((config.port as number) || 993);
          setImapUsername((config.username as string) || '');
          setImapPassword(''); // Don't pre-populate password for security
          setImapUseSsl((config.use_ssl as boolean) ?? true);
        } else if (provider.provider_type === 'microsoft') {
          setTenantId((config.tenant_id as string) || '');
          setClientId((config.client_id as string) || '');
          setClientSecret(''); // Don't pre-populate secret for security
          setUserEmail((config.user_email as string) || '');
        }
      }
    } catch (error) {
      console.error('Failed to fetch provider config:', error);
    }
  };

  const handleCancelEdit = () => {
    setEditingProviderId(null);
    setEmailProviderName('');
    setImapServer('');
    setImapUsername('');
    setImapPassword('');
  };

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

  const handleAddOrUpdateEmailProvider = () => {
    const providerData: EmailProviderCreate = {
      name: emailProviderName,
      provider_type: emailProviderType,
    };

    if (emailProviderType === 'microsoft') {
      providerData.tenant_id = tenantId;
      providerData.client_id = clientId;
      providerData.client_secret = clientSecret;
      providerData.user_email = userEmail;
    } else if (emailProviderType === 'imap') {
      providerData.server = imapServer;
      providerData.port = imapPort;
      providerData.username = imapUsername;
      providerData.password = imapPassword;
      providerData.use_ssl = imapUseSsl;
    }

    if (editingProviderId) {
      console.log('Updating email provider:', editingProviderId, providerData);
      updateEmailProviderMutation.mutate({ providerId: editingProviderId, data: providerData });
    } else {
      console.log('Adding email provider:', providerData);
      addEmailProviderMutation.mutate(providerData);
    }
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
      <PageHeader
        icon={SettingsIcon}
        title="Settings"
        subtitle="Configure your LLM provider and email integration"
      />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* LLM Settings */}
        <Card title="LLM Configuration">
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
        </Card>
      </div>

      {/* Email Configuration - Full Width */}
      <Card title="Email Configuration" icon={Mail}>
        {/* Existing Providers */}
        {emailProviders?.data?.providers && emailProviders.data.providers.length > 0 && (
          <div className="mb-6">
            <h4 className="text-sm font-medium text-gray-700 mb-2">Configured Providers</h4>
            <div className="space-y-2">
              {emailProviders.data.providers.map((provider: EmailProvider) => (
                <div key={provider.id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                  <div className="flex items-center gap-3">
                    <Mail className="h-5 w-5 text-gray-400" />
                    <div>
                      <p className="font-medium">{provider.name}</p>
                      <p className="text-sm text-gray-500">
                        {provider.provider_type === 'microsoft' ? 'Office 365 / Microsoft' :
                         provider.provider_type === 'gmail' ? 'Gmail' : 'IMAP'}
                        {' '}&bull;{' '}
                        <span className={provider.enabled ? 'text-green-600' : 'text-gray-400'}>
                          {provider.enabled ? 'Enabled' : 'Disabled'}
                        </span>
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => testEmailProviderMutation.mutate(provider.id)}
                      disabled={testEmailProviderMutation.isPending}
                      className="btn btn-secondary btn-sm flex items-center gap-1"
                      title="Test Connection"
                    >
                      <TestTube className="h-4 w-4" />
                      Test
                    </button>
                    <button
                      onClick={() => handleEditProvider(provider)}
                      className="btn btn-secondary btn-sm flex items-center gap-1"
                      title="Edit Provider"
                    >
                      <Pencil className="h-4 w-4" />
                      Edit
                    </button>
                    <button
                      onClick={() => deleteEmailProviderMutation.mutate(provider.id)}
                      disabled={deleteEmailProviderMutation.isPending}
                      className="btn btn-secondary btn-sm text-red-600 hover:bg-red-50"
                      title="Delete Provider"
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </div>
                </div>
              ))}
            </div>
            {testEmailProviderMutation.isSuccess && (
              <div className="mt-2 p-2 bg-green-50 text-green-700 text-sm rounded">
                {testEmailProviderMutation.data?.data?.message || 'Connection successful'}
              </div>
            )}
            {testEmailProviderMutation.isError && (
              <div className="mt-2 p-2 bg-red-50 text-red-700 text-sm rounded">
                Connection test failed
              </div>
            )}
          </div>
        )}

        {/* Add/Edit Provider */}
        <div className="border-t pt-4">
          <div className="flex items-center justify-between mb-3">
            <h4 className="text-sm font-medium text-gray-700">
              {editingProviderId ? 'Edit Email Provider' : 'Add Email Provider'}
            </h4>
            {editingProviderId && (
              <button
                onClick={handleCancelEdit}
                className="btn btn-secondary btn-sm flex items-center gap-1"
              >
                <X className="h-4 w-4" />
                Cancel
              </button>
            )}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Provider Name */}
            <div>
              <label className="label">Provider Name</label>
              <input
                type="text"
                className="input"
                placeholder="e.g., Company Email"
                value={emailProviderName}
                onChange={(e) => setEmailProviderName(e.target.value)}
              />
            </div>

            {/* Provider Type */}
            <div>
              <label className="label">Provider Type</label>
              <select
                className="select"
                value={emailProviderType}
                onChange={(e) => setEmailProviderType(e.target.value as 'microsoft' | 'gmail' | 'imap')}
              >
                <option value="microsoft">Office 365 / Microsoft</option>
                <option value="imap">IMAP</option>
                <option value="gmail">Gmail (requires OAuth setup)</option>
              </select>
            </div>
          </div>

          {/* Microsoft/Office 365 Configuration */}
          {emailProviderType === 'microsoft' && (
            <div className="mt-4 p-4 bg-blue-50 rounded-lg">
              <h5 className="font-medium text-blue-800 mb-3">Office 365 Configuration</h5>
              <p className="text-sm text-blue-700 mb-4">
                You need to register an app in Azure AD with Mail.Read permissions.{' '}
                <a
                  href="https://portal.azure.com/#blade/Microsoft_AAD_RegisteredApps/ApplicationsListBlade"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="underline flex items-center gap-1 inline"
                >
                  Azure Portal <ExternalLink className="h-3 w-3" />
                </a>
              </p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="label">Tenant ID</label>
                  <input
                    type="text"
                    className="input"
                    placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                    value={tenantId}
                    onChange={(e) => setTenantId(e.target.value)}
                  />
                </div>
                <div>
                  <label className="label">Client ID (Application ID)</label>
                  <input
                    type="text"
                    className="input"
                    placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
                    value={clientId}
                    onChange={(e) => setClientId(e.target.value)}
                  />
                </div>
                <div>
                  <label className="label">Client Secret</label>
                  <input
                    type="password"
                    className="input"
                    placeholder="Enter client secret"
                    value={clientSecret}
                    onChange={(e) => setClientSecret(e.target.value)}
                  />
                </div>
                <div>
                  <label className="label">User Email</label>
                  <input
                    type="email"
                    className="input"
                    placeholder="user@company.com"
                    value={userEmail}
                    onChange={(e) => setUserEmail(e.target.value)}
                  />
                </div>
              </div>
            </div>
          )}

          {/* IMAP Configuration */}
          {emailProviderType === 'imap' && (
            <div className="mt-4 p-4 bg-gray-50 rounded-lg">
              <h5 className="font-medium text-gray-800 mb-3">IMAP Configuration</h5>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="label">IMAP Server</label>
                  <input
                    type="text"
                    className="input"
                    placeholder="imap.example.com"
                    value={imapServer}
                    onChange={(e) => setImapServer(e.target.value)}
                  />
                </div>
                <div>
                  <label className="label">Port</label>
                  <input
                    type="number"
                    className="input"
                    value={imapPort}
                    onChange={(e) => setImapPort(parseInt(e.target.value))}
                  />
                </div>
                <div>
                  <label className="label">Username</label>
                  <input
                    type="text"
                    className="input"
                    placeholder="user@example.com"
                    value={imapUsername}
                    onChange={(e) => setImapUsername(e.target.value)}
                  />
                </div>
                <div>
                  <label className="label">Password</label>
                  <input
                    type="password"
                    className="input"
                    placeholder="Enter password"
                    value={imapPassword}
                    onChange={(e) => setImapPassword(e.target.value)}
                  />
                </div>
              </div>
              <div className="mt-3 flex items-center">
                <input
                  type="checkbox"
                  id="imapSsl"
                  checked={imapUseSsl}
                  onChange={(e) => setImapUseSsl(e.target.checked)}
                  className="mr-2"
                />
                <label htmlFor="imapSsl" className="text-sm text-gray-700">
                  Use SSL/TLS Connection
                </label>
              </div>
            </div>
          )}

          {/* Gmail Notice */}
          {emailProviderType === 'gmail' && (
            <div className="mt-4 p-4 bg-yellow-50 rounded-lg">
              <h5 className="font-medium text-yellow-800 mb-2">Gmail Configuration</h5>
              <p className="text-sm text-yellow-700">
                Gmail requires OAuth2 authentication. You need to set up a Google Cloud project
                and create OAuth credentials. This feature requires additional configuration in config.ini.
              </p>
            </div>
          )}

          {/* Add/Update Provider Button */}
          <div className="mt-4 flex items-center gap-4">
            <button
              onClick={handleAddOrUpdateEmailProvider}
              disabled={addEmailProviderMutation.isPending || updateEmailProviderMutation.isPending}
              className="btn btn-primary flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Save className="h-4 w-4 mr-2" />
              {editingProviderId
                ? (updateEmailProviderMutation.isPending ? 'Saving...' : 'Save Changes')
                : (addEmailProviderMutation.isPending ? 'Adding...' : 'Add Email Provider')}
            </button>
            {addEmailProviderMutation.isSuccess && addEmailProviderMutation.data?.data?.success && (
              <span className="text-green-600 text-sm flex items-center">
                <CheckCircle className="h-4 w-4 mr-1" />
                Provider added successfully
              </span>
            )}
            {updateEmailProviderMutation.isSuccess && updateEmailProviderMutation.data?.data?.success && (
              <span className="text-green-600 text-sm flex items-center">
                <CheckCircle className="h-4 w-4 mr-1" />
                Provider updated successfully
              </span>
            )}
            {addEmailProviderMutation.isError && (
              <span className="text-red-600 text-sm flex items-center">
                <AlertCircle className="h-4 w-4 mr-1" />
                {(addEmailProviderMutation.error as Error)?.message || 'Failed to add provider'}
              </span>
            )}
            {updateEmailProviderMutation.isError && (
              <span className="text-red-600 text-sm flex items-center">
                <AlertCircle className="h-4 w-4 mr-1" />
                {(updateEmailProviderMutation.error as Error)?.message || 'Failed to update provider'}
              </span>
            )}
            {addEmailProviderMutation.data && !addEmailProviderMutation.data.data?.success && (
              <span className="text-red-600 text-sm flex items-center">
                <AlertCircle className="h-4 w-4 mr-1" />
                {addEmailProviderMutation.data.data?.error || 'Failed to add provider'}
              </span>
            )}
          </div>
        </div>
      </Card>
    </div>
  );
}
