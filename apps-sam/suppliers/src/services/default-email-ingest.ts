/**
 * Default suppliers email-ingest adapter.
 *
 * Bridges SAM's `ctx.emailIngest` onto the suppliers plugin's
 * `supplierEmailAttachments` adapter shape:
 *
 *   fetchAttachment({ emailId, attachmentId? })
 *     → { text?, bytes? } | null
 *
 * Behaviour:
 *   - When `attachmentId` is supplied, calls `ctx.emailIngest
 *     .getAttachmentText(msg, attachmentId)` and returns
 *     { text, bytes? }
 *   - When `attachmentId` is NOT supplied, returns the email's plain
 *     body text (stripped of HTML if only HTML is available).
 *
 * Subscribes to claimed mailboxes via `registerHandler` and stores
 * each ingested message under a sequential numeric ID — same pattern
 * as the bank-reconcile and gocardless defaults. Activates when
 * `ctx.emailIngest` is wired AND the tenant configures at least one
 * mailbox via `ctx.config.mailboxes`.
 */
import type { SamEmailIngestService } from '../app-context.js';

interface CachedMessage {
  id: number;
  graphMessageId: string;
  raw: unknown;
  bodyText: string | null;
}

export interface SupplierEmailAttachmentsAdapter {
  fetchAttachment(opts: {
    emailId: number;
    attachmentId?: string;
  }): Promise<{ text?: string; bytes?: Uint8Array } | null>;
}

interface IngestOptions {
  emailIngest: SamEmailIngestService;
  /**
   * App ID. Used only to filter `onOwnershipChange` events.
   */
  appId: string;
  /**
   * Optional starter mailbox list. When omitted (the production
   * path), the adapter calls `listMyMailboxes()` itself.
   */
  initialMailboxes?: Array<{ id: string; email_address?: string | null }>;
  cacheSize?: number;
  logger?: {
    info: (m: string, ...a: unknown[]) => void;
    warn: (m: string, ...a: unknown[]) => void;
    error: (m: string, ...a: unknown[]) => void;
  };
}

function pickField<T = unknown>(obj: unknown, ...keys: string[]): T | undefined {
  if (!obj || typeof obj !== 'object') return undefined;
  for (const k of keys) {
    const v = (obj as Record<string, unknown>)[k];
    if (v !== undefined && v !== null) return v as T;
  }
  return undefined;
}

function stripHtml(s: string): string {
  return s
    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
    .replace(/<script[^>]*>[\s\S]*?<\/script>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function extractBodyText(raw: unknown): string | null {
  const direct = pickField<string>(raw, 'body_text', 'bodyText');
  if (direct) return direct;
  const directHtml = pickField<string>(raw, 'body_html', 'bodyHtml');
  if (directHtml) return stripHtml(directHtml);
  const body = pickField<unknown>(raw, 'body');
  if (body && typeof body === 'object') {
    const ct = pickField<string>(body, 'contentType', 'content_type');
    const content = pickField<string>(body, 'content');
    if (!content) return null;
    if (ct?.toLowerCase().includes('html')) return stripHtml(content);
    return content;
  }
  return null;
}

export interface DefaultEmailIngestAdapter {
  attachments: SupplierEmailAttachmentsAdapter;
  shutdown: () => Promise<void>;
}

export function createDefaultEmailIngestAdapter(
  options: IngestOptions,
): DefaultEmailIngestAdapter {
  const log = options.logger ?? console;
  const cap = options.cacheSize ?? 1_000;
  const cache = new Map<number, CachedMessage>();
  const byGraphId = new Map<string, number>();
  let nextId = 1;
  /** mailboxId → detach function returned by registerHandler */
  const handlers = new Map<string, () => void>();
  /** detach functions for ownership/activity subscriptions */
  const eventDetachers: Array<() => void> = [];

  function evictIfFull() {
    while (cache.size > cap) {
      const oldest = cache.keys().next().value as number | undefined;
      if (oldest === undefined) break;
      const m = cache.get(oldest);
      cache.delete(oldest);
      if (m) byGraphId.delete(m.graphMessageId);
    }
  }

  function ingest(raw: unknown): CachedMessage {
    const graphId =
      pickField<string>(raw, 'id', 'message_id', 'messageId') ?? '';
    if (graphId && byGraphId.has(graphId)) {
      const id = byGraphId.get(graphId)!;
      return cache.get(id)!;
    }
    const id = nextId++;
    const msg: CachedMessage = {
      id,
      graphMessageId: graphId,
      raw,
      bodyText: extractBodyText(raw),
    };
    cache.set(id, msg);
    if (graphId) byGraphId.set(graphId, id);
    evictIfFull();
    return msg;
  }

  function attachHandler(mailboxId: string): void {
    if (handlers.has(mailboxId)) return;
    const detach = options.emailIngest.registerHandler(
      mailboxId,
      (...args: unknown[]) => {
        ingest(args[0]);
        return undefined;
      },
    );
    handlers.set(mailboxId, detach);
  }

  function detachHandler(mailboxId: string): void {
    const d = handlers.get(mailboxId);
    if (d) {
      try {
        d();
      } catch {
        // ignore
      }
      handlers.delete(mailboxId);
    }
  }

  function applyMailboxList(
    rows: Array<{ id?: string; email_address?: string | null }>,
  ): void {
    for (const r of rows) {
      const id = typeof r.id === 'string' ? r.id : null;
      if (!id) continue;
      attachHandler(id);
    }
    log.info?.(
      `[suppliers email-ingest] attached to ${handlers.size} mailbox(es)`,
    );
  }

  if (options.initialMailboxes) {
    applyMailboxList(options.initialMailboxes);
  } else {
    Promise.resolve(options.emailIngest.listMyMailboxes())
      .then((rows) => {
        applyMailboxList(
          (rows as Array<Record<string, unknown>>).map((r) => ({
            id: typeof r.id === 'string' ? r.id : undefined,
            email_address:
              typeof r.email_address === 'string' ? r.email_address : null,
          })),
        );
      })
      .catch((err: unknown) => {
        log.warn?.(
          `[suppliers email-ingest] listMyMailboxes failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      });
  }

  try {
    const detachOwnership = options.emailIngest.onOwnershipChange(
      async (event: unknown) => {
        const e = event as {
          mailboxId?: string;
          previousOwnerAppId?: string | null;
          newOwnerAppId?: string | null;
        };
        if (!e?.mailboxId) return;
        if (
          e.newOwnerAppId === options.appId &&
          e.previousOwnerAppId !== options.appId
        ) {
          attachHandler(e.mailboxId);
        } else if (
          e.previousOwnerAppId === options.appId &&
          e.newOwnerAppId !== options.appId
        ) {
          detachHandler(e.mailboxId);
        }
      },
    );
    eventDetachers.push(detachOwnership);
  } catch {
    // optional in some SAM versions
  }

  const attachments: SupplierEmailAttachmentsAdapter = {
    async fetchAttachment({ emailId, attachmentId }) {
      const m = cache.get(emailId);
      if (!m) return null;
      if (!attachmentId) {
        return { text: m.bodyText ?? '' };
      }
      try {
        const result = await options.emailIngest.getAttachmentText(
          m.raw,
          attachmentId,
        );
        return { text: result.text };
      } catch (err) {
        log.error?.(
          `[suppliers email-ingest] getAttachmentText failed for email ${emailId}/${attachmentId}: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
        return null;
      }
    },
  };

  async function shutdown() {
    for (const d of eventDetachers.splice(0)) {
      try {
        d();
      } catch {
        // ignore
      }
    }
    for (const [, d] of handlers) {
      try {
        d();
      } catch {
        // ignore
      }
    }
    handlers.clear();
    cache.clear();
    byGraphId.clear();
  }

  return { attachments, shutdown };
}
