/**
 * Supplier communications log — record + list inbound/outbound contacts.
 *
 * Greenfield TS work (no Python equivalent — the supplier app was
 * never finished in Python).
 *
 * Used by the supplier-portal UI to show a timeline of contacts:
 *   - email scanned from inbox (channel='email')
 *   - phone call logged manually (channel='phone')
 *   - portal note (channel='portal')
 *
 * Stored in `supplier_communications` (per-app DB).
 */
import type { Knex } from 'knex';

export type CommunicationChannel = 'email' | 'phone' | 'portal';

const VALID_CHANNELS: ReadonlySet<CommunicationChannel> = new Set([
  'email',
  'phone',
  'portal',
]);

export interface CommunicationEntry {
  id: number;
  supplier_code: string;
  channel: CommunicationChannel;
  subject: string;
  content: string;
  sent_at: string; // ISO
}

function dateToIso(d: Date | string | null): string {
  if (!d) return '';
  if (d instanceof Date) {
    if (Number.isNaN(d.getTime())) return '';
    return d.toISOString();
  }
  return String(d);
}

// ---------------------------------------------------------------------
// list
// ---------------------------------------------------------------------

export interface ListCommunicationsOptions {
  supplierCode?: string | null;
  channel?: CommunicationChannel | null;
  fromDate?: string | null;
  toDate?: string | null;
  limit?: number;
}

export interface ListCommunicationsResponse {
  success: boolean;
  entries: CommunicationEntry[];
  count: number;
  error?: string;
}

export async function listCommunications(
  appDb: Knex,
  opts: ListCommunicationsOptions = {},
): Promise<ListCommunicationsResponse> {
  const limit = opts.limit ?? 200;
  if (
    opts.channel !== null &&
    opts.channel !== undefined &&
    !VALID_CHANNELS.has(opts.channel)
  ) {
    return {
      success: false,
      entries: [],
      count: 0,
      error: `channel must be one of: ${[...VALID_CHANNELS].join(', ')}`,
    };
  }

  try {
    let query = appDb('supplier_communications')
      .orderBy('sent_at', 'desc')
      .limit(limit);

    if (opts.supplierCode) {
      query = query.where({ supplier_code: opts.supplierCode });
    }
    if (opts.channel) {
      query = query.where({ channel: opts.channel });
    }
    if (opts.fromDate) {
      query = query.andWhere('sent_at', '>=', opts.fromDate);
    }
    if (opts.toDate) {
      query = query.andWhere('sent_at', '<=', opts.toDate);
    }

    const rows = (await query) as unknown as Array<{
      id: number;
      supplier_code: string;
      channel: string | null;
      subject: string | null;
      content: string | null;
      sent_at: Date | string | null;
    }>;

    const entries: CommunicationEntry[] = rows.map((r) => {
      const ch = (r.channel ?? '').toString() as CommunicationChannel;
      return {
        id: r.id,
        supplier_code: r.supplier_code,
        channel: VALID_CHANNELS.has(ch) ? ch : 'email',
        subject: r.subject ?? '',
        content: r.content ?? '',
        sent_at: dateToIso(r.sent_at),
      };
    });

    return { success: true, entries, count: entries.length };
  } catch (err: any) {
    return {
      success: false,
      entries: [],
      count: 0,
      error: err?.message ?? String(err),
    };
  }
}

// ---------------------------------------------------------------------
// record (insert a new communications entry)
// ---------------------------------------------------------------------

export interface RecordCommunicationInput {
  supplier_code: string;
  channel: string; // validated against VALID_CHANNELS
  subject?: string;
  content?: string;
  sent_at?: string; // optional override; defaults to now
}

export interface RecordCommunicationResponse {
  success: boolean;
  entry?: CommunicationEntry;
  error?: string;
}

export async function recordCommunication(
  appDb: Knex,
  input: RecordCommunicationInput,
): Promise<RecordCommunicationResponse> {
  const supplierCode = (input.supplier_code ?? '').trim();
  const channel = (input.channel ?? '').trim() as CommunicationChannel;
  if (!supplierCode) {
    return { success: false, error: 'supplier_code is required' };
  }
  if (!VALID_CHANNELS.has(channel)) {
    return {
      success: false,
      error: `channel must be one of: ${[...VALID_CHANNELS].join(', ')}`,
    };
  }

  try {
    const sentAt = input.sent_at ?? new Date().toISOString();
    const inserted = await appDb('supplier_communications')
      .insert({
        supplier_code: supplierCode,
        channel,
        subject: (input.subject ?? '').slice(0, 500),
        content: input.content ?? '',
        sent_at: input.sent_at ?? appDb.fn.now(),
      })
      .returning('id');

    const id =
      Array.isArray(inserted) && inserted.length > 0
        ? typeof inserted[0] === 'object'
          ? (inserted[0] as { id: number }).id
          : Number(inserted[0])
        : 0;

    return {
      success: true,
      entry: {
        id,
        supplier_code: supplierCode,
        channel,
        subject: input.subject ?? '',
        content: input.content ?? '',
        sent_at: sentAt,
      },
    };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}

// ---------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------

export interface DeleteCommunicationResponse {
  success: boolean;
  deleted?: boolean;
  error?: string;
}

export async function deleteCommunication(
  appDb: Knex,
  id: number,
): Promise<DeleteCommunicationResponse> {
  if (!Number.isFinite(id) || id <= 0) {
    return { success: false, error: 'id must be a positive number' };
  }
  try {
    const removed = await appDb('supplier_communications')
      .where({ id })
      .delete();
    return { success: true, deleted: Number(removed) > 0 };
  } catch (err: any) {
    return { success: false, error: err?.message ?? String(err) };
  }
}
