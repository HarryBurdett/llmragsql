import { describe, it, expect, vi } from 'vitest';
import { createDefaultEmailIngestAdapter } from '../src/services/default-email-ingest.js';
import type { SamEmailIngestService } from '../src/app-context.js';

interface FakeIngest extends SamEmailIngestService {
  push: (msg: unknown) => void;
}

function makeIngest(): FakeIngest {
  let handler: ((msg: unknown) => unknown) | null = null;
  return {
    async claimMailbox() {
      return { mailboxId: 'mb1' };
    },
    async releaseMailbox() {},
    async listMyMailboxes() {
      return [];
    },
    registerHandler(_id: string, fn) {
      handler = fn as (msg: unknown) => unknown;
      return () => {
        handler = null;
      };
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
    onOwnershipChange() {
      return () => undefined;
    },
    onActivityChange() {
      return () => undefined;
    },
    push(msg) {
      if (handler) handler(msg);
    },
  } as FakeIngest;
}

describe('createDefaultEmailIngestAdapter (suppliers)', () => {
  it('returns email body when no attachmentId given', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({
      id: 'g1',
      subject: 'Statement March',
      body_text: 'Total due: £500',
      receivedDateTime: '2026-04-01',
    });
    const r = await a.attachments.fetchAttachment({ emailId: 1 });
    expect(r?.text).toBe('Total due: £500');
    await a.shutdown();
  });

  it('strips HTML body when only HTML provided', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({
      id: 'g1',
      subject: 'X',
      body: { contentType: 'HTML', content: '<p>Hello <b>world</b></p>' },
      receivedDateTime: '2026-04-01',
    });
    const r = await a.attachments.fetchAttachment({ emailId: 1 });
    expect(r?.text).toBe('Hello world');
  });

  it('fetches attachment text via ctx.emailIngest', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({
      id: 'g1',
      subject: 'X',
      receivedDateTime: '2026-04-01',
    });
    const r = await a.attachments.fetchAttachment({
      emailId: 1,
      attachmentId: 'att-7',
    });
    expect(r?.text).toBe('text-of-att-7');
    expect(ingest.getAttachmentText).toHaveBeenCalledTimes(1);
  });

  it('returns null for unknown emailId', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({ emailIngest: ingest });
    expect(await a.attachments.fetchAttachment({ emailId: 999 })).toBeNull();
  });

  it('returns null when ctx.emailIngest throws', async () => {
    const ingest = makeIngest();
    (ingest.getAttachmentText as any).mockRejectedValue(new Error('boom'));
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
      logger: { info() {}, warn() {}, error() {} },
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({ id: 'g1', subject: 'X', receivedDateTime: '2026-04-01' });
    const r = await a.attachments.fetchAttachment({
      emailId: 1,
      attachmentId: 'a',
    });
    expect(r).toBeNull();
  });

  it('dedupes by graph message id', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({ id: 'g1', subject: 'X', body_text: 'first' });
    ingest.push({ id: 'g1', subject: 'X', body_text: 'second-IGNORED' });
    const r = await a.attachments.fetchAttachment({ emailId: 1 });
    expect(r?.text).toBe('first');
  });
});
