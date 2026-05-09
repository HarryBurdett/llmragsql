import { describe, it, expect, vi } from 'vitest';
import { createDefaultEmailIngestAdapter } from '../src/services/default-email-ingest.js';
import type { SamEmailIngestService } from '../src/app-context.js';

interface FakeIngest extends SamEmailIngestService {
  push: (msg: unknown, mailboxId?: string) => void;
  fireOwnership: (event: {
    mailboxId: string;
    previousOwnerAppId?: string | null;
    newOwnerAppId?: string | null;
  }) => Promise<void>;
}

function makeIngest(opts: {
  myMailboxes?: Array<{ id: string; email_address: string }>;
} = {}): FakeIngest {
  const handlersByMailbox = new Map<string, (msg: unknown) => unknown>();
  const ownershipListeners: Array<(event: unknown) => Promise<void>> = [];
  return {
    async claimMailbox() {
      return { mailboxId: 'mb-claim' };
    },
    async releaseMailbox() {},
    async listMyMailboxes() {
      return opts.myMailboxes ?? [];
    },
    registerHandler(id: string, fn) {
      handlersByMailbox.set(id, fn as (msg: unknown) => unknown);
      return () => handlersByMailbox.delete(id);
    },
    fetchAttachment: vi.fn(async () => ({
      bytes: Buffer.from(''),
      name: 'x',
      contentType: 'application/pdf',
    })),
    getAttachmentText: vi.fn(async (_msg: unknown, attId: string) => ({
      name: `att-${attId}`,
      contentType: 'text/plain',
      text: `text-of-${attId}`,
      truncated: false,
    })),
    onOwnershipChange(fn) {
      ownershipListeners.push(fn as (event: unknown) => Promise<void>);
      return () => undefined;
    },
    onActivityChange() {
      return () => undefined;
    },
    push(msg, mailboxId = 'mb1') {
      const h = handlersByMailbox.get(mailboxId);
      if (h) h(msg);
    },
    async fireOwnership(event) {
      for (const l of ownershipListeners) await l(event);
    },
  } as FakeIngest;
}

describe('createDefaultEmailIngestAdapter (suppliers)', () => {
  it('returns email body when no attachmentId given (listMyMailboxes path)', async () => {
    const ingest = makeIngest({
      myMailboxes: [{ id: 'mb1', email_address: 'ops@example.com' }],
    });
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      appId: 'suppliers',
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push(
      {
        id: 'g1',
        subject: 'Statement March',
        body_text: 'Total due: £500',
        receivedDateTime: '2026-04-01',
      },
      'mb1',
    );
    const r = await a.attachments.fetchAttachment({ emailId: 1 });
    expect(r?.text).toBe('Total due: £500');
    await a.shutdown();
  });

  it('strips HTML body when only HTML provided', async () => {
    const ingest = makeIngest({
      myMailboxes: [{ id: 'mb1', email_address: 'ops@example.com' }],
    });
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      appId: 'suppliers',
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push(
      {
        id: 'g1',
        subject: 'X',
        body: { contentType: 'HTML', content: '<p>Hello <b>world</b></p>' },
        receivedDateTime: '2026-04-01',
      },
      'mb1',
    );
    const r = await a.attachments.fetchAttachment({ emailId: 1 });
    expect(r?.text).toBe('Hello world');
  });

  it('fetches attachment text via ctx.emailIngest', async () => {
    const ingest = makeIngest({
      myMailboxes: [{ id: 'mb1', email_address: 'ops@example.com' }],
    });
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      appId: 'suppliers',
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push(
      { id: 'g1', subject: 'X', receivedDateTime: '2026-04-01' },
      'mb1',
    );
    const r = await a.attachments.fetchAttachment({
      emailId: 1,
      attachmentId: 'att-7',
    });
    expect(r?.text).toBe('text-of-att-7');
    expect(ingest.getAttachmentText).toHaveBeenCalledTimes(1);
  });

  it('attaches when SAM Admin assigns a mailbox', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      appId: 'suppliers',
    });
    await new Promise((r) => setTimeout(r, 5));
    await ingest.fireOwnership({
      mailboxId: 'mb-new',
      previousOwnerAppId: null,
      newOwnerAppId: 'suppliers',
    });
    ingest.push(
      {
        id: 'g1',
        subject: 'X',
        body_text: 'after-assign',
        receivedDateTime: '2026-04-01',
      },
      'mb-new',
    );
    const r = await a.attachments.fetchAttachment({ emailId: 1 });
    expect(r?.text).toBe('after-assign');
  });

  it('test-path initialMailboxes bypasses listMyMailboxes', async () => {
    const ingest = makeIngest();
    const listSpy = vi.spyOn(ingest, 'listMyMailboxes');
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      appId: 'suppliers',
      initialMailboxes: [{ id: 'mb-test', email_address: 'test@x' }],
    });
    expect(listSpy).not.toHaveBeenCalled();
    ingest.push(
      {
        id: 'g1',
        subject: 'X',
        body_text: 'via-init',
        receivedDateTime: '2026-04-01',
      },
      'mb-test',
    );
    const r = await a.attachments.fetchAttachment({ emailId: 1 });
    expect(r?.text).toBe('via-init');
  });

  it('returns null for unknown emailId', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      appId: 'suppliers',
    });
    expect(await a.attachments.fetchAttachment({ emailId: 999 })).toBeNull();
  });

  it('returns null when ctx.emailIngest throws', async () => {
    const ingest = makeIngest({
      myMailboxes: [{ id: 'mb1', email_address: 'ops@example.com' }],
    });
    (ingest.getAttachmentText as any).mockRejectedValue(new Error('boom'));
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      appId: 'suppliers',
      logger: { info() {}, warn() {}, error() {} },
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push(
      { id: 'g1', subject: 'X', receivedDateTime: '2026-04-01' },
      'mb1',
    );
    const r = await a.attachments.fetchAttachment({ emailId: 1, attachmentId: 'a' });
    expect(r).toBeNull();
  });

  it('dedupes by graph message id', async () => {
    const ingest = makeIngest({
      myMailboxes: [{ id: 'mb1', email_address: 'ops@example.com' }],
    });
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      appId: 'suppliers',
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({ id: 'g1', subject: 'X', body_text: 'first' }, 'mb1');
    ingest.push({ id: 'g1', subject: 'X', body_text: 'second-IGNORED' }, 'mb1');
    const r = await a.attachments.fetchAttachment({ emailId: 1 });
    expect(r?.text).toBe('first');
  });
});
