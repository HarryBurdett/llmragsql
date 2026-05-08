/**
 * Read-only "System Connection" panel.
 *
 * Shown on every app's Settings page (Bank Rec, GoCardless,
 * Suppliers). Surfaces the centralised parameters this app
 * instance is wired up to — Opera SQL, IMAP, SMTP, AI provider,
 * active company, Opera version — so the operator can see at a
 * glance what backends are configured without leaving the app.
 *
 * Design choices:
 *
 *   - **Read-only**: this is a diagnostic display, not an editor.
 *     Centralised config is set via env vars (today) or SAM
 *     (tomorrow). Per-app duplication of these values would invite
 *     drift — see docs/sam-migration/sam-integration-pattern.md.
 *
 *   - **No secrets shown**: passwords, API keys, and tokens are
 *     reported as `configured: true|false` indicators only. The
 *     backend's /api/system/connection-info endpoint redacts them.
 *
 *   - **Same shape pre-/post-SAM**: the values displayed have the
 *     same structure regardless of source. SAM-day is a backend
 *     change only.
 */
import { useQuery } from '@tanstack/react-query';
import {
  Activity, AlertCircle, CheckCircle2, Database, Inbox, Mail, Send,
  Brain, Building2, Server, ShieldAlert,
} from 'lucide-react';
import { authFetch } from '../api/client';

interface ConnectionInfo {
  active_company: {
    id: string;
    name: string;
    opera_version: string;
  };
  opera_sql: {
    server: string;
    port: string;
    database: string;
    username: string;
    use_windows_auth: boolean;
    ssl: boolean;
    password_configured: boolean;
  };
  opera3: {
    data_path: string;
    write_agent_url: string;
    applies_when: string;
  };
  email_provider?: {
    provider: string;
    microsoft_tenant_id_configured: boolean;
    microsoft_client_id_configured: boolean;
    microsoft_client_secret_configured: boolean;
  };
  email_mailbox?: {
    mailbox: string;
    source: string;
  };
  email_imap: {
    enabled: boolean;
    server: string;
    port: string;
    username: string;
    use_ssl: boolean;
    password_configured: boolean;
  };
  email_smtp: {
    server: string;
    port: string;
    username: string;
    from_address: string;
    from_address_source?: string;
    password_configured: boolean;
  };
  ai_provider: {
    provider: string;
    embedding_model: string;
    gemini_model: string;
    gemini_configured: boolean;
    openai_configured: boolean;
    anthropic_configured: boolean;
    groq_configured: boolean;
  };
  deployment: {
    app_name: string;
    installed_apps: string;
    sam_enabled: boolean;
  };
}

interface SystemConnectionPanelProps {
  /**
   * Display label for the app this panel is being rendered inside —
   * e.g. "Bank Reconciliation", "GoCardless", "Suppliers".
   *
   * Used to label the active mailbox so the operator can see exactly
   * which inbox THIS app reads from — important when a customer has
   * separate inboxes per workflow (banking@, payments@, ap@) but one
   * shared set of MS Graph credentials.
   */
  appLabel?: string;
}

export function SystemConnectionPanel({ appLabel }: SystemConnectionPanelProps = {}) {
  const { data, isLoading, error } = useQuery<ConnectionInfo>({
    queryKey: ['system-connection-info'],
    queryFn: async () => {
      const r = await authFetch('http://localhost:8000/api/system/connection-info');
      if (!r.ok) throw new Error(`Failed: ${r.statusText}`);
      return r.json();
    },
    refetchOnWindowFocus: false,
  });

  if (isLoading) {
    return (
      <div className="rounded-lg border border-gray-200 bg-white p-4">
        <p className="text-sm text-gray-500">Loading system connection…</p>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 p-4">
        <p className="text-sm text-red-800 flex items-center gap-2">
          <AlertCircle className="w-4 h-4" />
          Could not load system connection info.
        </p>
      </div>
    );
  }

  const isOpera3 = data.active_company.opera_version === '3';

  return (
    <div className="rounded-lg border border-gray-200 bg-white">
      <div className="px-4 py-3 border-b border-gray-200 bg-gray-50 rounded-t-lg flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Server className="w-4 h-4 text-gray-600" />
          <h3 className="font-medium text-gray-800 text-sm">System Connection</h3>
        </div>
        <span className="text-xs text-gray-500">
          Configured at <code className="text-gray-700">Admin → Application Settings</code>
        </span>
      </div>

      <div className="p-4 space-y-4 text-sm">

        {/* Active company + Opera version */}
        <Section icon={Building2} label="Active Company">
          <Row k="Name" v={data.active_company.name || '(none selected)'} />
          <Row k="Opera version" v={data.active_company.opera_version} />
        </Section>

        {/* Opera SQL — only shown for SE */}
        {!isOpera3 && (
          <Section icon={Database} label="Opera SQL Server (SE)">
            <Row k="Server" v={`${data.opera_sql.server}:${data.opera_sql.port}` || '—'} />
            <Row k="Database" v={data.opera_sql.database || '—'} />
            <Row k="Username" v={data.opera_sql.username || '—'} />
            <SecretRow k="Password" configured={data.opera_sql.password_configured} />
            <Row k="Windows auth" v={data.opera_sql.use_windows_auth ? 'yes' : 'no'} />
            <Row k="SSL" v={data.opera_sql.ssl ? 'yes' : 'no'} />
          </Section>
        )}

        {/* Opera 3 — shown when active */}
        {isOpera3 && (
          <Section icon={Database} label="Opera 3 (FoxPro)">
            <Row k="Data path" v={data.opera3.data_path || '—'} />
            <Row k="Write Agent" v={data.opera3.write_agent_url || '—'} />
          </Section>
        )}

        {/* Email Mailbox — per-app identity (NEW) */}
        <Section
          icon={Inbox}
          label={appLabel ? `Mailbox (${appLabel})` : 'Mailbox (this app)'}
        >
          <Row
            k="Address"
            v={data.email_mailbox?.mailbox || '—'}
          />
          <Row
            k="Configured via"
            v={data.email_mailbox?.source || 'EMAIL_MAILBOX (per app)'}
          />
        </Section>

        {/* Email Provider — central per customer */}
        {data.email_provider && (
          <Section icon={Mail} label="Email Provider (central)">
            <Row k="Provider" v={data.email_provider.provider || 'imap'} />
            {data.email_provider.provider === 'microsoft' && (
              <>
                <SecretRow
                  k="MS tenant ID"
                  configured={data.email_provider.microsoft_tenant_id_configured}
                />
                <SecretRow
                  k="MS client ID"
                  configured={data.email_provider.microsoft_client_id_configured}
                />
                <SecretRow
                  k="MS client secret"
                  configured={data.email_provider.microsoft_client_secret_configured}
                />
              </>
            )}
          </Section>
        )}

        {/* Email IMAP — only when provider is IMAP */}
        {(!data.email_provider || data.email_provider.provider === 'imap') && (
          <Section icon={Mail} label="Email IMAP (receive)">
            <Row k="Status" v={data.email_imap.enabled ? 'enabled' : 'disabled'} />
            <Row k="Server" v={`${data.email_imap.server}:${data.email_imap.port}` || '—'} />
            <Row k="Username" v={data.email_imap.username || '—'} />
            <SecretRow k="Password" configured={data.email_imap.password_configured} />
            <Row k="SSL" v={data.email_imap.use_ssl ? 'yes' : 'no'} />
          </Section>
        )}

        {/* Email SMTP */}
        <Section icon={Send} label="Email SMTP (send)">
          <Row k="Server" v={`${data.email_smtp.server}:${data.email_smtp.port}` || '—'} />
          <Row k="Username" v={data.email_smtp.username || '—'} />
          <Row k="From" v={data.email_smtp.from_address || '—'} />
          {data.email_smtp.from_address_source && (
            <Row k="From source" v={data.email_smtp.from_address_source} />
          )}
          <SecretRow k="Password" configured={data.email_smtp.password_configured} />
        </Section>

        {/* AI provider */}
        <Section icon={Brain} label="AI Provider">
          <Row k="Active provider" v={data.ai_provider.provider} />
          <Row k="Embedding model" v={data.ai_provider.embedding_model} />
          {data.ai_provider.provider === 'gemini' && (
            <Row k="Gemini model" v={data.ai_provider.gemini_model} />
          )}
          <SecretRow k="Gemini key" configured={data.ai_provider.gemini_configured} />
          {data.ai_provider.openai_configured && (
            <SecretRow k="OpenAI key" configured={true} />
          )}
          {data.ai_provider.anthropic_configured && (
            <SecretRow k="Anthropic key" configured={true} />
          )}
          {data.ai_provider.groq_configured && (
            <SecretRow k="Groq key" configured={true} />
          )}
        </Section>

        {/* Deployment context */}
        <Section icon={Activity} label="Deployment">
          <Row k="App name" v={data.deployment.app_name} />
          <Row k="Installed apps" v={data.deployment.installed_apps} />
          <Row
            k="SAM platform"
            v={data.deployment.sam_enabled ? 'enabled' : 'not yet (using local config)'}
          />
        </Section>

      </div>
    </div>
  );
}

function Section({
  icon: Icon, label, children,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <div className="flex items-center gap-2 mb-1.5">
        <Icon className="w-3.5 h-3.5 text-gray-500" />
        <span className="text-xs uppercase tracking-wide text-gray-500 font-medium">
          {label}
        </span>
      </div>
      <div className="ml-5 space-y-0.5">{children}</div>
    </div>
  );
}

function Row({ k, v }: { k: string; v: string }) {
  return (
    <div className="flex items-baseline gap-2 text-xs">
      <span className="text-gray-500 min-w-[110px]">{k}</span>
      <span className="font-mono text-gray-800">{v || '—'}</span>
    </div>
  );
}

function SecretRow({ k, configured }: { k: string; configured: boolean }) {
  const Icon = configured ? CheckCircle2 : ShieldAlert;
  const cls = configured ? 'text-green-600' : 'text-amber-600';
  return (
    <div className="flex items-baseline gap-2 text-xs">
      <span className="text-gray-500 min-w-[110px]">{k}</span>
      <span className={`flex items-center gap-1 ${cls}`}>
        <Icon className="w-3.5 h-3.5" />
        {configured ? 'configured' : 'not configured'}
      </span>
    </div>
  );
}
