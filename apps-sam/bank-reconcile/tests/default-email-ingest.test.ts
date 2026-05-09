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
    async releaseMailbox() {
      // noop
    },
    async listMyMailboxes() {
      return [];
    },
    registerHandler(_id: string, fn) {
      handler = fn as (msg: unknown) => unknown;
      return () => {
        handler = null;
      };
    },
    fetchAttachment: vi.fn(async (_msg: unknown, attId: string) => ({
      bytes: Buffer.from(`bytes-for-${attId}`),
      name: `att-${attId}.pdf`,
      contentType: 'application/pdf',
    })),
    async getAttachmentText() {
      return { name: 'x', contentType: 'application/pdf', text: '', truncated: false };
    },
    onOwnershipChange() {
      return () => undefined;
    },
    onActivityChange() {
      return () => undefined;
    },
    push(msg: unknown) {
      if (handler) handler(msg);
    },
  } as FakeIngest;
}

describe('createDefaultEmailIngestAdapter', () => {
  it('returns mailbox + attachments + shutdown', () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({ emailIngest: ingest });
    expect(typeof a.mailbox.list).toBe('function');
    expect(typeof a.attachments.fetchAttachment).toBe('function');
    expect(typeof a.shutdown).toBe('function');
  });

  it('caches incoming messages and lists them by date filter', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    // Allow the async claim+register chain to settle
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({
      id: 'graph-1',
      subject: 'Bank statement',
      from: { emailAddress: { address: 'bank@x.com' } },
      receivedDateTime: '2026-04-15T09:00:00Z',
      attachments: [{ id: 'att-a', name: 'stmt.pdf', size: 1234 }],
    });
    ingest.push({
      id: 'graph-2',
      subject: 'Old',
      receivedDateTime: '2020-01-01T00:00:00Z',
      attachments: [],
    });

    const r = await a.mailbox.list({
      fromDate: new Date('2026-01-01'),
      pageSize: 10,
    });
    expect(r.emails.length).toBe(1);
    expect(r.emails[0]?.subject).toBe('Bank statement');
    expect(r.emails[0]?.attachments?.[0]?.attachment_id).toBe('att-a');

    const byId = await a.mailbox.getById(r.emails[0]!.id);
    expect(byId?.subject).toBe('Bank statement');
    await a.shutdown();
  });

  it('fetchAttachment returns null for unknown emailId', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({ emailIngest: ingest });
    const r = await a.attachments.fetchAttachment({
      emailId: 999,
      attachmentId: 'x',
    });
    expect(r).toBeNull();
  });

  it('fetchAttachment proxies through ctx.emailIngest', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({
      id: 'graph-1',
      subject: 'X',
      receivedDateTime: '2026-04-15T09:00:00Z',
      attachments: [{ id: 'att-1', name: 'a.pdf' }],
    });
    const list = await a.mailbox.list({
      fromDate: new Date('2026-01-01'),
      pageSize: 5,
    });
    const id = list.emails[0]!.id;
    const r = await a.attachments.fetchAttachment({
      emailId: id,
      attachmentId: 'att-1',
    });
    expect(r?.filename).toBe('att-att-1.pdf');
    expect(Buffer.from(r!.bytes).toString('utf8')).toBe('bytes-for-att-1');
    expect(ingest.fetchAttachment).toHaveBeenCalledTimes(1);
  });

  it('dedupes by graph message id', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({ id: 'graph-1', subject: 'A', receivedDateTime: '2026-04-15' });
    ingest.push({ id: 'graph-1', subject: 'A', receivedDateTime: '2026-04-15' });
    const list = await a.mailbox.list({
      fromDate: new Date('2026-01-01'),
      pageSize: 10,
    });
    expect(list.emails.length).toBe(1);
  });

  it('evicts old messages above cacheSize', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
      cacheSize: 2,
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({ id: 'g1', subject: '1', receivedDateTime: '2026-04-01' });
    ingest.push({ id: 'g2', subject: '2', receivedDateTime: '2026-04-02' });
    ingest.push({ id: 'g3', subject: '3', receivedDateTime: '2026-04-03' });
    const list = await a.mailbox.list({
      fromDate: new Date('2026-01-01'),
      pageSize: 10,
    });
    expect(list.emails.length).toBe(2);
    expect(list.emails.map((e) => e.subject).sort()).toEqual(['2', '3']);
  });
});
