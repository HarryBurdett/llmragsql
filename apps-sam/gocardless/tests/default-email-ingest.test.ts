import { describe, it, expect } from 'vitest';
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
    async fetchAttachment() {
      return { bytes: Buffer.from(''), name: 'x', contentType: 'application/pdf' };
    },
    async getAttachmentText() {
      return { name: 'x', contentType: 'text/plain', text: '', truncated: false };
    },
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

describe('createDefaultEmailIngestAdapter (gocardless)', () => {
  it('lists ingested messages with body_text extracted from Graph body', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({
      id: 'g1',
      subject: 'GoCardless payout £1,234.56',
      receivedDateTime: '2026-04-15T09:00:00Z',
      body: { contentType: 'Text', content: 'Net amount: £1,234.56' },
      from: { emailAddress: { address: 'noreply@gocardless.com' } },
    });
    const r = await a.mailbox.list({
      search: '',
      fromDate: new Date('2026-01-01'),
      pageSize: 10,
    });
    expect(r.emails.length).toBe(1);
    expect(r.emails[0]?.body_text).toContain('Net amount');
    expect(r.emails[0]?.from_address).toBe('noreply@gocardless.com');
    await a.shutdown();
  });

  it('filters by search keyword in subject + body', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({
      id: 'g1',
      subject: 'GoCardless payout',
      receivedDateTime: '2026-04-15T09:00:00Z',
      body_text: 'gross amount 100',
    });
    ingest.push({
      id: 'g2',
      subject: 'Random newsletter',
      receivedDateTime: '2026-04-15T09:00:00Z',
      body_text: 'unrelated',
    });
    const r = await a.mailbox.list({
      search: 'payout',
      fromDate: new Date('2026-01-01'),
      pageSize: 10,
    });
    expect(r.emails.length).toBe(1);
    expect(r.emails[0]?.subject).toBe('GoCardless payout');
  });

  it('filters by toDate', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({ id: 'g1', subject: 'A', receivedDateTime: '2026-04-15' });
    ingest.push({ id: 'g2', subject: 'B', receivedDateTime: '2026-06-15' });
    const r = await a.mailbox.list({
      search: '',
      fromDate: new Date('2026-01-01'),
      toDate: new Date('2026-05-01'),
      pageSize: 10,
    });
    expect(r.emails.map((e) => e.subject).sort()).toEqual(['A']);
  });

  it('dedupes by graph message id', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    ingest.push({ id: 'g1', subject: 'A', receivedDateTime: '2026-04-15' });
    ingest.push({ id: 'g1', subject: 'A', receivedDateTime: '2026-04-15' });
    const r = await a.mailbox.list({
      search: '',
      fromDate: new Date('2026-01-01'),
      pageSize: 10,
    });
    expect(r.emails.length).toBe(1);
  });

  it('honours pageSize', async () => {
    const ingest = makeIngest();
    const a = createDefaultEmailIngestAdapter({
      emailIngest: ingest,
      mailboxes: ['ops@example.com'],
    });
    await new Promise((r) => setTimeout(r, 5));
    for (let i = 0; i < 5; i++) {
      ingest.push({
        id: `g${i}`,
        subject: `S${i}`,
        receivedDateTime: `2026-04-1${i}`,
      });
    }
    const r = await a.mailbox.list({
      search: '',
      fromDate: new Date('2026-01-01'),
      pageSize: 2,
    });
    expect(r.emails.length).toBe(2);
  });
});
