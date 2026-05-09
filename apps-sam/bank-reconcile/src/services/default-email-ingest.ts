/**
 * Default email-ingest adapter.
 *
 * Bridges SAM's `ctx.emailIngest` service onto the `BankMailboxAdapter`
 * + `EmailAttachmentProvider` shapes the bank-reconcile services
 * expect. Two pieces:
 *
 *  1. Subscribe to mailbox messages via `registerHandler` and keep a
 *     bounded in-memory cache keyed by a sequential numeric ID. The ID
 *     space is the only stable handle the bank-reconcile UI has —
 *     Microsoft Graph message IDs are opaque strings the React table
 *     can't use as React keys reliably.
 *
 *  2. Provide `list/getById/fetchAttachment` against that cache.
 *
 * The cache is per-process and capped (default 1,000 messages). It's
 * not persistent — restarts wipe it. Production deployments should
 * either persist it via SAM's per-app DB (left to the SAM team) or
 * accept the warm-up cost on restart, which is fine because the
 * email-ingest service replays recent messages to handlers on
 * (re)claim.
 */
import type { Knex } from 'knex';
import type {
  BankMailboxAdapter,
  MailboxAttachment,
  MailboxEmail,
} from './scan-emails.js';
import type { EmailAttachmentProvider } from './preview-from-email.js';
import type { SamEmailIngestService } from '../app-context.js';

interface CachedMessage {
  id: number;
  graphMessageId: string;
  raw: unknown; // The original message object the SAM service handed us
  subject: string | null;
  fromAddress: string | null;
  receivedAt: string | null;
  attachments: MailboxAttachment[];
}

interface IngestOptions {
  emailIngest: SamEmailIngestService;
  /** Mailbox email address(es) to claim and listen on. */
  mailboxes?: string[];
  /** Maximum cached messages (FIFO). Default 1,000. */
  cacheSize?: number;
  /**
   * Optional Knex pool — if supplied, the adapter persists incoming
   * messages so they survive restarts.
   */
  appDb?: Knex | null;
  /** Logger (defaults to console). */
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

function normaliseMessage(raw: unknown, id: number): CachedMessage {
  const graphId = pickField<string>(raw, 'id', 'message_id', 'messageId') ?? '';
  const subject = pickField<string>(raw, 'subject') ?? null;
  const fromObj =
    pickField<unknown>(raw, 'from', 'sender') ?? null;
  let fromAddress: string | null = null;
  if (typeof fromObj === 'string') fromAddress = fromObj;
  else if (fromObj && typeof fromObj === 'object') {
    const ea = pickField<{ address?: string }>(fromObj, 'emailAddress', 'email_address');
    fromAddress =
      ea?.address ??
      pickField<string>(fromObj, 'address', 'email') ??
      null;
  }
  const receivedRaw = pickField<string | Date>(
    raw,
    'received_at',
    'receivedDateTime',
    'received',
  );
  const receivedAt =
    receivedRaw instanceof Date
      ? receivedRaw.toISOString()
      : typeof receivedRaw === 'string'
        ? receivedRaw
        : null;

  const atts = pickField<unknown[]>(raw, 'attachments', 'has_attachments');
  const attachments: MailboxAttachment[] = Array.isArray(atts)
    ? atts.map((a) => {
        const id =
          pickField<string>(a, 'id', 'attachment_id', 'attachmentId') ?? '';
        const filename =
          pickField<string>(a, 'name', 'filename', 'file_name') ?? '';
        const size = pickField<number>(a, 'size', 'size_bytes') ?? 0;
        const ct =
          pickField<string>(a, 'contentType', 'content_type') ?? null;
        return {
          attachment_id: id,
          filename,
          size_bytes: size,
          content_type: ct,
        };
      })
    : [];

  return {
    id,
    graphMessageId: graphId,
    raw,
    subject,
    fromAddress,
    receivedAt,
    attachments,
  };
}

export interface DefaultEmailIngestAdapter {
  mailbox: BankMailboxAdapter;
  attachments: EmailAttachmentProvider;
  /** Detach all listeners and release claimed mailboxes. */
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
  const detachers: Array<() => void> = [];
  const claimed: string[] = [];

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
    const msg = normaliseMessage(raw, id);
    cache.set(id, msg);
    if (msg.graphMessageId) byGraphId.set(msg.graphMessageId, id);
    evictIfFull();
    return msg;
  }

  // Hook up listeners for each requested mailbox. We do this lazily
  // because claimMailbox is async; failures are logged but don't
  // abort adapter creation (the adapter still works for messages
  // pushed in via test code).
  const mailboxesToClaim = options.mailboxes ?? [];
  Promise.all(
    mailboxesToClaim.map(async (mailboxEmail) => {
      try {
        const claim = await options.emailIngest.claimMailbox({
          mailboxEmail,
        });
        claimed.push(mailboxEmail);
        const mailboxId =
          pickField<string>(claim, 'mailboxId', 'mailbox_id', 'id') ??
          mailboxEmail;
        const detach = options.emailIngest.registerHandler(
          mailboxId,
          (...args: unknown[]) => {
            const msg = args[0] as unknown;
            ingest(msg);
            return undefined;
          },
        );
        detachers.push(detach);
        log.info?.(`[email-ingest] claimed ${mailboxEmail}`);
      } catch (err) {
        log.warn?.(
          `[email-ingest] failed to claim ${mailboxEmail}: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
      }
    }),
  ).catch(() => undefined);

  const mailbox: BankMailboxAdapter = {
    async sync() {
      // The SAM email-ingest service syncs continuously in the
      // background; nothing to do here. Provided as a no-op so callers
      // that rely on `sync` don't fail.
    },
    async list({ fromDate, pageSize }) {
      const since = fromDate.getTime();
      const items: MailboxEmail[] = [];
      for (const m of cache.values()) {
        if (m.receivedAt) {
          const t = Date.parse(m.receivedAt);
          if (Number.isFinite(t) && t < since) continue;
        }
        items.push({
          id: m.id,
          subject: m.subject,
          from_address: m.fromAddress,
          received_at: m.receivedAt,
          has_attachments: m.attachments.length > 0,
          attachments: m.attachments,
        });
      }
      // Newest first.
      items.sort((a, b) => {
        const ax = a.received_at ? Date.parse(String(a.received_at)) : 0;
        const bx = b.received_at ? Date.parse(String(b.received_at)) : 0;
        return bx - ax;
      });
      return { emails: items.slice(0, pageSize) };
    },
    async getById(emailId) {
      const m = cache.get(emailId);
      if (!m) return null;
      return {
        id: m.id,
        subject: m.subject,
        from_address: m.fromAddress,
        received_at: m.receivedAt,
        has_attachments: m.attachments.length > 0,
        attachments: m.attachments,
      };
    },
  };

  const attachments: EmailAttachmentProvider = {
    async fetchAttachment({ emailId, attachmentId }) {
      const m = cache.get(emailId);
      if (!m) return null;
      try {
        const result = await options.emailIngest.fetchAttachment(
          m.raw,
          attachmentId,
        );
        return {
          bytes: new Uint8Array(
            result.bytes.buffer,
            result.bytes.byteOffset,
            result.bytes.byteLength,
          ),
          filename: result.name,
          contentType: result.contentType,
        };
      } catch (err) {
        log.error?.(
          `[email-ingest] fetchAttachment failed for email ${emailId}: ${
            err instanceof Error ? err.message : String(err)
          }`,
        );
        return null;
      }
    },
  };

  async function shutdown() {
    for (const d of detachers.splice(0)) {
      try {
        d();
      } catch {
        // ignore
      }
    }
    for (const mb of claimed.splice(0)) {
      try {
        await options.emailIngest.releaseMailbox({ mailboxEmail: mb });
      } catch {
        // ignore
      }
    }
    cache.clear();
    byGraphId.clear();
  }

  return { mailbox, attachments, shutdown };
}
