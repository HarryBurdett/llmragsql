/**
 * Stubs for SAM-injected services that the standalone runner doesn't
 * fully implement. Plugins that try to use these get sensible "not
 * available" responses (mostly 503).
 *
 * Email-ingest and LLM are the big ones — they're significant
 * services in SAM that we can't trivially recreate. For testing
 * read-only and non-email-dependent workflows, the stubs are enough.
 * Email-dependent flows (scan-emails, etc.) will return helpful
 * "service not wired in test runner" messages.
 */

export const stubLogger = {
  info: (...a: unknown[]) => console.log('[plugin]', ...a),
  warn: (...a: unknown[]) => console.warn('[plugin]', ...a),
  error: (...a: unknown[]) => console.error('[plugin]', ...a),
  debug: (...a: unknown[]) => console.debug('[plugin]', ...a),
};

export const stubEmailIngest = {
  async claimMailbox() {
    throw new Error('emailIngest not available in standalone test runner — use legacy backend on port 8000 for email scanning');
  },
  async releaseMailbox() { /* noop */ },
  async listMyMailboxes(): Promise<unknown[]> {
    return [];
  },
  registerHandler() {
    return () => { /* noop */ };
  },
  async fetchAttachment() {
    throw new Error('emailIngest not available in standalone test runner');
  },
  async getAttachmentText() {
    throw new Error('emailIngest not available in standalone test runner');
  },
  onOwnershipChange() {
    return () => { /* noop */ };
  },
  onActivityChange() {
    return () => { /* noop */ };
  },
};

export const stubLlm = {
  async *chat() {
    yield { error: 'LLM not available in standalone test runner' };
  },
  async *stream() {
    yield { error: 'LLM not available in standalone test runner' };
  },
};

export const stubEmail = {
  async send() {
    return { success: false, error: 'email.send not available in standalone test runner' };
  },
  async isConfigured() {
    return false;
  },
};

export const stubGraph = {
  async getToken() {
    throw new Error('Graph token service not available in standalone test runner');
  },
};
