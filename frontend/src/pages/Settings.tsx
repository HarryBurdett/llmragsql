import { useState, useEffect, useRef } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Save, RefreshCw, CheckCircle, AlertCircle, ExternalLink, Mail, Trash2, TestTube, Database, Server, CreditCard, ChevronDown, Search, Pencil, X } from 'lucide-react';
import apiClient from '../api/client';
import type { ProviderConfig, DatabaseConfig, EmailProviderCreate, EmailProvider, OperaConfig, Opera3Company } from '../api/client';

// Searchable dropdown component for settings
interface SearchableDropdownOption {
  code: string;
  description: string;
  rate?: number;
}

interface SearchableDropdownProps {
  options: SearchableDropdownOption[];
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  formatOption?: (opt: SearchableDropdownOption) => string;
}

function SearchableDropdown({ options, value, onChange, placeholder = 'Search...', formatOption }: SearchableDropdownProps) {
  const [search, setSearch] = useState('');
  const [isOpen, setIsOpen] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(0);
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const filtered = options.filter(opt => {
    const searchLower = search.toLowerCase();
    return opt.code.toLowerCase().includes(searchLower) ||
           opt.description.toLowerCase().includes(searchLower);
  });

  const selectedOption = options.find(o => o.code === value);
  const displayValue = selectedOption
    ? (formatOption ? formatOption(selectedOption) : `${selectedOption.code} - ${selectedOption.description}`)
    : '';

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false);
        setSearch('');
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  useEffect(() => {
    setHighlightedIndex(0);
  }, [search]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (!isOpen) {
      if (e.key === 'ArrowDown' || e.key === 'Enter') {
        setIsOpen(true);
        e.preventDefault();
      }
      return;
    }

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setHighlightedIndex(prev => Math.min(prev + 1, filtered.length - 1));
        break;
      case 'ArrowUp':
        e.preventDefault();
        setHighlightedIndex(prev => Math.max(prev - 1, 0));
        break;
      case 'Enter':
        e.preventDefault();
        if (filtered[highlightedIndex]) {
          onChange(filtered[highlightedIndex].code);
          setIsOpen(false);
          setSearch('');
        }
        break;
      case 'Escape':
        setIsOpen(false);
        setSearch('');
        break;
    }
  };

  const handleSelect = (code: string) => {
    onChange(code);
    setIsOpen(false);
    setSearch('');
  };

  // Limit displayed options to improve performance with large lists
  const displayedOptions = filtered.slice(0, 100);

  return (
    <div ref={containerRef} className="relative" style={{ zIndex: isOpen ? 9999 : 'auto' }}>
      <div
        className="select flex items-center justify-between cursor-pointer"
        onClick={() => {
          setIsOpen(!isOpen);
          if (!isOpen) setTimeout(() => inputRef.current?.focus(), 0);
        }}
      >
        <span className={value ? 'text-gray-900' : 'text-gray-400'}>
          {displayValue || placeholder}
        </span>
        <ChevronDown className="h-4 w-4 text-gray-400" />
      </div>

      {isOpen && (
        <div
          className="absolute left-0 w-full mt-1 bg-white border border-gray-200 rounded-md shadow-lg"
          style={{ zIndex: 9999, maxHeight: '300px' }}
        >
          <div className="p-2 border-b border-gray-100 sticky top-0 bg-white">
            <div className="relative">
              <Search className="absolute left-2 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
              <input
                ref={inputRef}
                type="text"
                className="w-full pl-8 pr-3 py-1.5 text-sm border border-gray-200 rounded focus:outline-none focus:ring-1 focus:ring-blue-500"
                placeholder="Type to search..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                onKeyDown={handleKeyDown}
              />
            </div>
          </div>
          <div className="overflow-y-auto" style={{ maxHeight: '240px' }}>
            {displayedOptions.length === 0 ? (
              <div className="px-3 py-2 text-sm text-gray-500">No matches found</div>
            ) : (
              <>
                {displayedOptions.map((opt, idx) => (
                  <div
                    key={opt.code}
                    className={`px-3 py-2 text-sm cursor-pointer ${
                      idx === highlightedIndex ? 'bg-blue-50 text-blue-700' : 'hover:bg-gray-50'
                    } ${opt.code === value ? 'bg-blue-100' : ''}`}
                    onClick={() => handleSelect(opt.code)}
                    onMouseEnter={() => setHighlightedIndex(idx)}
                  >
                    {formatOption ? formatOption(opt) : `${opt.code} - ${opt.description}`}
                  </div>
                ))}
                {filtered.length > 100 && (
                  <div className="px-3 py-2 text-xs text-gray-400 italic">
                    Showing first 100 of {filtered.length} results. Type to narrow down.
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// GoCardless Settings Component
function GoCardlessSettings() {
  const [batchTypes, setBatchTypes] = useState<{ code: string; description: string }[]>([]);
  const [nominalAccounts, setNominalAccounts] = useState<{ code: string; description: string }[]>([]);
  const [vatCodes, setVatCodes] = useState<{ code: string; description: string; rate: number }[]>([]);
  const [paymentTypes, setPaymentTypes] = useState<{ code: string; description: string }[]>([]);
  const [bankAccounts, setBankAccounts] = useState<{ code: string; description: string }[]>([]);

  const [defaultBatchType, setDefaultBatchType] = useState('');
  const [defaultBankCode, setDefaultBankCode] = useState('BC010');
  const [feesNominalAccount, setFeesNominalAccount] = useState('');
  const [feesVatCode, setFeesVatCode] = useState('');
  const [feesPaymentType, setFeesPaymentType] = useState('');
  const [archiveFolder, setArchiveFolder] = useState('Archive/GoCardless');
  const [isSaving, setIsSaving] = useState(false);
  const [saveResult, setSaveResult] = useState<{ success: boolean; message: string } | null>(null);

  // Load all dropdown options and current settings
  useEffect(() => {
    // Fetch batch types
    fetch('/api/gocardless/batch-types')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.batch_types) {
          setBatchTypes(data.batch_types);
        }
      })
      .catch(err => console.error('Failed to load batch types:', err));

    // Fetch nominal accounts
    fetch('/api/gocardless/nominal-accounts')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.accounts) {
          setNominalAccounts(data.accounts);
        }
      })
      .catch(err => console.error('Failed to load nominal accounts:', err));

    // Fetch VAT codes
    fetch('/api/gocardless/vat-codes')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.codes) {
          setVatCodes(data.codes);
        }
      })
      .catch(err => console.error('Failed to load VAT codes:', err));

    // Fetch payment types
    fetch('/api/gocardless/payment-types')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.types) {
          setPaymentTypes(data.types);
        }
      })
      .catch(err => console.error('Failed to load payment types:', err));

    // Fetch bank accounts
    fetch('/api/gocardless/bank-accounts')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.accounts) {
          setBankAccounts(data.accounts);
        }
      })
      .catch(err => console.error('Failed to load bank accounts:', err));

    // Fetch current settings
    fetch('/api/gocardless/settings')
      .then(res => res.json())
      .then(data => {
        if (data.success && data.settings) {
          setDefaultBatchType(data.settings.default_batch_type || '');
          setDefaultBankCode(data.settings.default_bank_code || 'BC010');
          setFeesNominalAccount(data.settings.fees_nominal_account || '');
          setFeesVatCode(data.settings.fees_vat_code || '');
          setFeesPaymentType(data.settings.fees_payment_type || '');
          setArchiveFolder(data.settings.archive_folder || 'Archive/GoCardless');
        }
      })
      .catch(err => console.error('Failed to load GoCardless settings:', err));
  }, []);

  const handleSave = async () => {
    setIsSaving(true);
    setSaveResult(null);
    try {
      const response = await fetch('/api/gocardless/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          default_batch_type: defaultBatchType,
          default_bank_code: defaultBankCode,
          fees_nominal_account: feesNominalAccount,
          fees_vat_code: feesVatCode,
          fees_payment_type: feesPaymentType,
          archive_folder: archiveFolder
        })
      });
      const data = await response.json();
      setSaveResult({ success: data.success, message: data.success ? 'Settings saved' : data.error });
    } catch (error) {
      setSaveResult({ success: false, message: 'Failed to save settings' });
    } finally {
      setIsSaving(false);
    }
  };

  return (
    <div className="card" style={{ overflow: 'visible' }}>
      <div className="flex items-center gap-2 mb-4">
        <CreditCard className="h-5 w-5 text-green-600" />
        <h3 className="text-lg font-semibold">GoCardless Import Settings</h3>
      </div>

      <div className="space-y-4">
        <div>
          <label className="label">Default Cashbook Type (Batched Receipt)</label>
          <SearchableDropdown
            options={batchTypes}
            value={defaultBatchType}
            onChange={setDefaultBatchType}
            placeholder="-- Select Default Type --"
          />
          <p className="text-xs text-gray-500 mt-1">
            The cashbook receipt type to use for GoCardless batch imports. Must be a batched receipt type in Opera.
          </p>
        </div>

        <div>
          <label className="label">Default Bank Account</label>
          <SearchableDropdown
            options={bankAccounts}
            value={defaultBankCode}
            onChange={setDefaultBankCode}
            placeholder="-- Select Bank Account --"
          />
          <p className="text-xs text-gray-500 mt-1">
            The default bank account to receive GoCardless payments.
          </p>
        </div>

        <div className="border-t pt-4 mt-4">
          <h4 className="font-medium text-gray-800 mb-3">GoCardless Fees Posting</h4>
          <p className="text-xs text-gray-500 mb-3">
            Configure where GoCardless fees and VAT should be posted in the nominal ledger.
          </p>

          <div className="grid grid-cols-3 gap-4" style={{ overflow: 'visible' }}>
            <div style={{ overflow: 'visible' }}>
              <label className="label">Fees Nominal Account</label>
              <SearchableDropdown
                options={nominalAccounts}
                value={feesNominalAccount}
                onChange={setFeesNominalAccount}
                placeholder="-- Select Account --"
              />
              <p className="text-xs text-gray-500 mt-1">Where GoCardless charges are posted</p>
            </div>
            <div style={{ overflow: 'visible' }}>
              <label className="label">VAT Code</label>
              <SearchableDropdown
                options={vatCodes}
                value={feesVatCode}
                onChange={setFeesVatCode}
                placeholder="-- Select VAT Code --"
                formatOption={(opt) => `${opt.code} - ${opt.description} (${opt.rate}%)`}
              />
              <p className="text-xs text-gray-500 mt-1">VAT rate for fees (usually Standard)</p>
            </div>
            <div style={{ overflow: 'visible' }}>
              <label className="label">Cashbook Payment Type</label>
              <SearchableDropdown
                options={paymentTypes}
                value={feesPaymentType}
                onChange={setFeesPaymentType}
                placeholder="-- Select Type --"
              />
              <p className="text-xs text-gray-500 mt-1">Nominal payment type for fees</p>
            </div>
          </div>
        </div>

        <div className="border-t border-gray-200 pt-4 mt-4">
          <h4 className="font-medium text-gray-800 mb-3">Email Integration</h4>
          <div>
            <label className="label">Archive Folder</label>
            <input
              type="text"
              className="input"
              placeholder="Archive/GoCardless"
              value={archiveFolder}
              onChange={(e) => setArchiveFolder(e.target.value)}
            />
            <p className="text-xs text-gray-500 mt-1">
              IMAP folder to move imported GoCardless emails to. Created automatically if it doesn't exist.
            </p>
          </div>
        </div>

        <div className="flex space-x-3 pt-2">
          <button
            onClick={handleSave}
            disabled={isSaving}
            className="btn btn-primary flex items-center"
          >
            <Save className="h-4 w-4 mr-2" />
            {isSaving ? 'Saving...' : 'Save GoCardless Settings'}
          </button>
        </div>

        {saveResult && (
          <div className={`flex items-center text-sm ${saveResult.success ? 'text-green-600' : 'text-red-600'}`}>
            {saveResult.success ? <CheckCircle className="h-4 w-4 mr-1" /> : <AlertCircle className="h-4 w-4 mr-1" />}
            {saveResult.message}
          </div>
        )}
      </div>
    </div>
  );
}

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

  // Opera Settings State
  const [operaVersion, setOperaVersion] = useState<'sql_se' | 'opera3'>('sql_se');
  const [opera3ServerPath, setOpera3ServerPath] = useState('');
  const [opera3BasePath, setOpera3BasePath] = useState('C:\\Apps\\O3 Server VFP');
  const [opera3CompanyCode, setOpera3CompanyCode] = useState('');

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

  // Opera configuration queries
  const { data: operaConfig } = useQuery({
    queryKey: ['operaConfig'],
    queryFn: () => apiClient.getOperaConfig(),
  });

  const { data: opera3Companies, refetch: refetchOpera3Companies } = useQuery({
    queryKey: ['opera3Companies'],
    queryFn: () => apiClient.getOpera3Companies(),
    enabled: operaVersion === 'opera3' && !!opera3BasePath,
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

  // Load Opera config into state
  useEffect(() => {
    if (operaConfig?.data) {
      const cfg = operaConfig.data;
      if (cfg.version) setOperaVersion(cfg.version);
      if (cfg.opera3_server_path) setOpera3ServerPath(cfg.opera3_server_path);
      if (cfg.opera3_base_path) setOpera3BasePath(cfg.opera3_base_path);
      if (cfg.opera3_company_code) setOpera3CompanyCode(cfg.opera3_company_code);
    }
  }, [operaConfig]);

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

  const operaMutation = useMutation({
    mutationFn: (data: OperaConfig) => apiClient.updateOperaConfig(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['operaConfig'] });
      queryClient.invalidateQueries({ queryKey: ['opera3Companies'] });
    },
  });

  const operaTestMutation = useMutation({
    mutationFn: (data: OperaConfig) => apiClient.testOperaConnection(data),
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

  const handleSaveOpera = () => {
    operaMutation.mutate({
      version: operaVersion,
      opera3_server_path: operaVersion === 'opera3' ? opera3ServerPath : undefined,
      opera3_base_path: operaVersion === 'opera3' ? opera3BasePath : undefined,
      opera3_company_code: operaVersion === 'opera3' ? opera3CompanyCode : undefined,
    });
  };

  const handleTestOpera = () => {
    operaTestMutation.mutate({
      version: operaVersion,
      opera3_server_path: operaVersion === 'opera3' ? opera3ServerPath : undefined,
      opera3_base_path: operaVersion === 'opera3' ? opera3BasePath : undefined,
      opera3_company_code: operaVersion === 'opera3' ? opera3CompanyCode : undefined,
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

      {/* Opera Configuration - Full Width */}
      <div className="card">
        <div className="flex items-center gap-2 mb-4">
          <Server className="h-5 w-5 text-purple-600" />
          <h3 className="text-lg font-semibold">Opera Configuration</h3>
        </div>

        <div className="space-y-4">
          {/* Version Selection */}
          <div>
            <label className="label">Opera Version</label>
            <select
              className="select"
              value={operaVersion}
              onChange={(e) => setOperaVersion(e.target.value as 'sql_se' | 'opera3')}
            >
              <option value="sql_se">Opera SQL SE (SQL Server)</option>
              <option value="opera3">Opera 3 (FoxPro/DBF)</option>
            </select>
            <p className="text-xs text-gray-500 mt-1">
              {operaVersion === 'sql_se'
                ? 'Uses SQL Server connection from Database Configuration above'
                : 'Uses FoxPro DBF files directly from the Opera 3 installation folder'}
            </p>
          </div>

          {/* SQL SE Info */}
          {operaVersion === 'sql_se' && (
            <div className="bg-blue-50 p-4 rounded-md">
              <div className="flex items-start gap-2">
                <Database className="h-5 w-5 text-blue-600 mt-0.5" />
                <div>
                  <p className="font-medium text-blue-800">Using SQL Server Connection</p>
                  <p className="text-sm text-blue-700 mt-1">
                    Opera SQL SE uses the SQL Server database configured above.
                    {dbServer && dbDatabase && (
                      <span className="block mt-1">
                        Currently connected to: <strong>{dbServer}</strong> / <strong>{dbDatabase}</strong>
                      </span>
                    )}
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* Opera 3 Settings */}
          {operaVersion === 'opera3' && (
            <>
              <div>
                <label className="label">Opera 3 Server Path</label>
                <input
                  type="text"
                  className="input"
                  placeholder="\\\\SERVER\\O3 Server VFP"
                  value={opera3ServerPath}
                  onChange={(e) => setOpera3ServerPath(e.target.value)}
                />
                <p className="text-xs text-gray-500 mt-1">
                  UNC or network path to the Opera 3 server installation (e.g., \\SERVER\O3 Server VFP)
                </p>
              </div>

              <div>
                <label className="label">Opera 3 Local Data Path</label>
                <input
                  type="text"
                  className="input"
                  placeholder="C:\Apps\O3 Server VFP"
                  value={opera3BasePath}
                  onChange={(e) => setOpera3BasePath(e.target.value)}
                />
                <p className="text-xs text-gray-500 mt-1">
                  Local path to the Opera 3 data folder (used if server path is not set)
                </p>
              </div>

              <div>
                <label className="label">Company</label>
                <div className="flex gap-2">
                  <select
                    className="select flex-1"
                    value={opera3CompanyCode}
                    onChange={(e) => setOpera3CompanyCode(e.target.value)}
                  >
                    <option value="">Select a company...</option>
                    {opera3Companies?.data?.companies?.map((company: Opera3Company) => (
                      <option key={company.code} value={company.code}>
                        {company.code} - {company.name}
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={() => refetchOpera3Companies()}
                    className="btn btn-secondary flex items-center"
                    title="Refresh company list"
                  >
                    <RefreshCw className="h-4 w-4" />
                  </button>
                </div>
                {opera3Companies?.data?.error && (
                  <p className="text-xs text-red-600 mt-1">
                    {opera3Companies.data.error}
                  </p>
                )}
              </div>
            </>
          )}

          {/* Actions */}
          <div className="flex space-x-3 pt-2">
            <button
              onClick={handleSaveOpera}
              disabled={operaMutation.isPending}
              className="btn btn-primary flex items-center"
            >
              <Save className="h-4 w-4 mr-2" />
              {operaMutation.isPending ? 'Saving...' : 'Save Opera Config'}
            </button>
            <button
              onClick={handleTestOpera}
              disabled={operaTestMutation.isPending}
              className="btn btn-secondary flex items-center"
            >
              {operaTestMutation.isPending ? 'Testing...' : 'Test Connection'}
            </button>
          </div>

          {/* Status Messages */}
          {operaMutation.isSuccess && (
            <div className="flex items-center text-green-600 text-sm">
              <CheckCircle className="h-4 w-4 mr-1" />
              Opera configuration saved successfully
            </div>
          )}
          {operaMutation.isError && (
            <div className="flex items-center text-red-600 text-sm">
              <AlertCircle className="h-4 w-4 mr-1" />
              Failed to save Opera configuration
            </div>
          )}
          {operaTestMutation.isSuccess && (
            <div className={`p-3 rounded-md text-sm ${
              operaTestMutation.data?.data?.success
                ? 'bg-green-50 text-green-800'
                : 'bg-red-50 text-red-800'
            }`}>
              {operaTestMutation.data?.data?.success ? (
                <div className="flex items-center">
                  <CheckCircle className="h-4 w-4 mr-2" />
                  {operaTestMutation.data?.data?.message}
                </div>
              ) : (
                <div className="flex items-center">
                  <AlertCircle className="h-4 w-4 mr-2" />
                  {operaTestMutation.data?.data?.error || 'Connection test failed'}
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* GoCardless Configuration */}
      <GoCardlessSettings />

      {/* Email Configuration - Full Width */}
      <div className="card">
        <div className="flex items-center gap-2 mb-4">
          <Mail className="h-5 w-5 text-blue-600" />
          <h3 className="text-lg font-semibold">Email Configuration</h3>
        </div>

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
      </div>
    </div>
  );
}
