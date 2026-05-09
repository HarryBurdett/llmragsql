import { describe, it, expect, vi } from 'vitest';
import {
  processStatement,
  acknowledgeStatement,
  approveStatement,
  editStatementResponse,
  bulkApproveStatements,
  type EmailSender,
  type OperaSupplierLookup,
  type PtranLookup,
  type PtranLine,
} from '../src/services/statement-actions.js';

interface SupplierStatementRow {
  id: number;
  supplier_code: string;
  status: string;
  statement_date: string | null;
  received_date: string | null;
  sender_email: string | null;
  acknowledged_at: string | null;
  response_text: string | null;
  response_subject: string | null;
  email_pdf_path: string | null;
  approved_by?: string | null;
  approved_at?: string | null;
  sent_at?: string | null;
  processed_at?: string | null;
}

interface State {
  statements: SupplierStatementRow[];
  lines: Array<{
    id: number;
    statement_id: number;
    reference: string | null;
    amount: number;
    match_status: string | null;
    status: string | null;
    matched_opera_ref: string | null;
  }>;
  contacts: Array<{
    id: number;
    supplier_code: string;
    contact_email: string | null;
    is_statement_contact: boolean;
    never_communicate: boolean;
  }>;
  automationSettings: Record<string, string>;
  comms: Array<{
    supplier_code: string;
    subject: string;
    content: string;
  }>;
}

function makeAppDb(state: State): any {
  function tableBuilder(table: string) {
    let mode: 'where-id' | 'where-key' | 'where-supplier' | 'where-line' | 'count' = 'where-id';
    let idFilter: number | null = null;
    let keyFilter: string | null = null;
    let supplierFilter: string | null = null;
    let neverCommFilter: boolean | null = null;
    let isStmtContactFilter: boolean | null = null;
    let lineMatchFilter: string | null = null;
    let statementIdFilter: number | null = null;
    let countMode = false;

    const builder: any = {
      where: (cond: any) => {
        if (typeof cond === 'object') {
          if ('id' in cond) {
            idFilter = cond.id;
            mode = 'where-id';
          }
          if ('key' in cond) {
            keyFilter = cond.key;
            mode = 'where-key';
          }
          if ('supplier_code' in cond) {
            supplierFilter = cond.supplier_code;
            mode = 'where-supplier';
            if ('never_communicate' in cond) neverCommFilter = cond.never_communicate;
            if ('is_statement_contact' in cond) isStmtContactFilter = cond.is_statement_contact;
          }
          if ('statement_id' in cond) {
            statementIdFilter = cond.statement_id;
            mode = 'where-line';
            if ('match_status' in cond) lineMatchFilter = cond.match_status;
          }
        }
        return builder;
      },
      whereNotNull: () => builder,
      first: async () => {
        if (table === 'supplier_statements' && idFilter !== null) {
          return state.statements.find((s) => s.id === idFilter);
        }
        if (table === 'supplier_automation_settings' && keyFilter) {
          const v = state.automationSettings[keyFilter];
          return v !== undefined ? { value: v } : undefined;
        }
        if (table === 'supplier_contacts_ext' && supplierFilter) {
          if (neverCommFilter === true) {
            const c = state.contacts.find(
              (c) => c.supplier_code === supplierFilter && c.never_communicate,
            );
            return c ? { id: c.id } : undefined;
          }
          if (isStmtContactFilter === true) {
            const c = state.contacts.find(
              (c) => c.supplier_code === supplierFilter && c.is_statement_contact && c.contact_email,
            );
            return c ? { contact_email: c.contact_email } : undefined;
          }
        }
        if (table === 'statement_lines' && countMode) {
          const c = state.lines.filter(
            (l) =>
              l.statement_id === statementIdFilter &&
              (lineMatchFilter ? l.match_status === lineMatchFilter : true),
          ).length;
          return { total: c };
        }
        return undefined;
      },
      count: () => {
        countMode = true;
        return builder;
      },
      update: async (payload: any) => {
        if (table === 'supplier_statements' && idFilter !== null) {
          const idx = state.statements.findIndex((s) => s.id === idFilter);
          if (idx >= 0) {
            state.statements[idx] = { ...state.statements[idx]!, ...payload };
            return 1;
          }
        }
        if (table === 'statement_lines' && idFilter !== null) {
          const idx = state.lines.findIndex((l) => l.id === idFilter);
          if (idx >= 0) {
            state.lines[idx] = { ...state.lines[idx]!, ...payload };
            return 1;
          }
        }
        return 0;
      },
      then: async (resolve: any) => {
        if (table === 'statement_lines' && statementIdFilter !== null) {
          const out = state.lines.filter((l) => l.statement_id === statementIdFilter);
          return resolve(out);
        }
        return resolve([]);
      },
      insert: async (payload: any) => {
        if (table === 'supplier_communications') {
          state.comms.push({
            supplier_code: payload.supplier_code,
            subject: payload.subject,
            content: payload.content,
          });
          return [state.comms.length];
        }
        return [1];
      },
    };
    return builder;
  }

  const db: any = (table: string) => tableBuilder(table);
  db.fn = { now: () => '__NOW__' };
  return db;
}

function makeEmail(opts: { failOn?: string } = {}): EmailSender {
  return {
    send: vi.fn(async ({ to }) => {
      if (opts.failOn && to === opts.failOn) {
        return { success: false, error: 'SMTP rejected recipient' };
      }
      return { success: true };
    }),
  };
}

const supplierLookup: OperaSupplierLookup = {
  resolveName: async (code) => `Acme ${code}`,
};

// ---------------------------------------------------------------------
// processStatement
// ---------------------------------------------------------------------

describe('processStatement', () => {
  const ptran: PtranLookup = {
    forSupplier: async () =>
      [
        {
          pt_unique: 'PTRAN-1',
          pt_trref: 'INV/123',
          pt_supref: 'SUP-INV-123',
          pt_trtype: 'I',
          pt_trvalue: 100,
          pt_trbal: 100,
          pt_trdate: '2026-04-01',
        },
      ] satisfies PtranLine[],
  };

  it('moves statement from received to queued and matches lines', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A001',
          status: 'received',
          statement_date: '2026-04-30',
          received_date: '2026-04-30T10:00:00Z',
          sender_email: 'ap@acme.com',
          acknowledged_at: null,
          response_text: null,
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [
        {
          id: 10,
          statement_id: 1,
          reference: 'INV/123',
          amount: 100,
          match_status: null,
          status: null,
          matched_opera_ref: null,
        },
        {
          id: 11,
          statement_id: 1,
          reference: 'INV/999',
          amount: 50,
          match_status: null,
          status: null,
          matched_opera_ref: null,
        },
      ],
      contacts: [],
      automationSettings: {},
      comms: [],
    };
    const result = await processStatement(makeAppDb(state), 1, ptran);
    expect(result.success).toBe(true);
    expect(result.matched).toBe(1);
    expect(result.query).toBe(1);
    expect(state.statements[0]?.status).toBe('queued');
    expect(state.lines[0]?.match_status).toBe('matched');
    expect(state.lines[0]?.matched_opera_ref).toBe('PTRAN-1');
    expect(state.lines[1]?.match_status).toBe('query');
  });

  it('rejects statements not in receivable state', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A001',
          status: 'sent',
          statement_date: null,
          received_date: null,
          sender_email: null,
          acknowledged_at: null,
          response_text: null,
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [],
      contacts: [],
      automationSettings: {},
      comms: [],
    };
    const result = await processStatement(makeAppDb(state), 1, ptran);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/cannot be processed/);
  });
});

// ---------------------------------------------------------------------
// acknowledgeStatement
// ---------------------------------------------------------------------

describe('acknowledgeStatement', () => {
  it('sends acknowledgement and updates status', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A001',
          status: 'received',
          statement_date: '2026-04-30',
          received_date: '2026-04-30T10:00:00Z',
          sender_email: 'ap@acme.com',
          acknowledged_at: null,
          response_text: null,
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [],
      contacts: [],
      automationSettings: {},
      comms: [],
    };
    const email = makeEmail();
    const result = await acknowledgeStatement(
      makeAppDb(state),
      email,
      supplierLookup,
      1,
    );
    expect(result.success).toBe(true);
    expect(result.email_sent).toBe(true);
    expect(result.recipient).toBe('ap@acme.com');
    expect(state.statements[0]?.status).toBe('acknowledged');
    expect(state.comms.length).toBe(1);
  });

  it('blocks already-acknowledged statements', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A001',
          status: 'acknowledged',
          statement_date: '2026-04-30',
          received_date: '2026-04-30T10:00:00Z',
          sender_email: 'ap@acme.com',
          acknowledged_at: '2026-04-30T11:00:00Z',
          response_text: null,
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [],
      contacts: [],
      automationSettings: {},
      comms: [],
    };
    const result = await acknowledgeStatement(
      makeAppDb(state),
      makeEmail(),
      supplierLookup,
      1,
    );
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/already been acknowledged/);
  });

  it('honours never_communicate policy', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A001',
          status: 'received',
          statement_date: '2026-04-30',
          received_date: null,
          sender_email: 'ap@acme.com',
          acknowledged_at: null,
          response_text: null,
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [],
      contacts: [
        {
          id: 1,
          supplier_code: 'A001',
          contact_email: 'ap@acme.com',
          is_statement_contact: true,
          never_communicate: true,
        },
      ],
      automationSettings: {},
      comms: [],
    };
    const result = await acknowledgeStatement(
      makeAppDb(state),
      makeEmail(),
      supplierLookup,
      1,
    );
    expect(result.success).toBe(false);
    expect(result.policy_blocked).toBe(true);
  });

  it('returns earliest_send_at when delay still active', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A001',
          status: 'received',
          statement_date: null,
          received_date: new Date().toISOString(),
          sender_email: 'ap@acme.com',
          acknowledged_at: null,
          response_text: null,
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [],
      contacts: [],
      automationSettings: { acknowledgment_delay_minutes: '120' },
      comms: [],
    };
    const result = await acknowledgeStatement(
      makeAppDb(state),
      makeEmail(),
      supplierLookup,
      1,
    );
    expect(result.success).toBe(false);
    expect(result.earliest_send_at).toBeDefined();
  });
});

// ---------------------------------------------------------------------
// approveStatement
// ---------------------------------------------------------------------

describe('approveStatement', () => {
  it('approves a queued statement and sends response', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A001',
          status: 'queued',
          statement_date: '2026-04-30',
          received_date: '2026-04-30T10:00:00Z',
          sender_email: 'ap@acme.com',
          acknowledged_at: '2026-04-30T11:00:00Z',
          response_text: 'Reconciled — please pay £100 owing.',
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [],
      contacts: [],
      automationSettings: {},
      comms: [],
    };
    const result = await approveStatement(
      makeAppDb(state),
      makeEmail(),
      supplierLookup,
      1,
      { approvedBy: 'admin' },
    );
    expect(result.success).toBe(true);
    expect(result.email_sent).toBe(true);
    expect(state.statements[0]?.status).toBe('sent');
    expect(state.statements[0]?.approved_by).toBe('admin');
    expect(state.comms.length).toBe(1);
  });

  it('rejects approval from invalid status', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A001',
          status: 'sent',
          statement_date: null,
          received_date: null,
          sender_email: null,
          acknowledged_at: null,
          response_text: null,
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [],
      contacts: [],
      automationSettings: {},
      comms: [],
    };
    const result = await approveStatement(
      makeAppDb(state),
      makeEmail(),
      supplierLookup,
      1,
      { approvedBy: 'admin' },
    );
    expect(result.success).toBe(false);
  });
});

// ---------------------------------------------------------------------
// editStatementResponse
// ---------------------------------------------------------------------

describe('editStatementResponse', () => {
  it('updates response_text and returns refreshed statement', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A001',
          status: 'queued',
          statement_date: '2026-04-30',
          received_date: null,
          sender_email: null,
          acknowledged_at: null,
          response_text: null,
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [],
      contacts: [],
      automationSettings: {},
      comms: [],
    };
    const result = await editStatementResponse(makeAppDb(state), 1, {
      responseText: 'New text',
    });
    expect(result.success).toBe(true);
    expect(state.statements[0]?.response_text).toBe('New text');
  });
});

// ---------------------------------------------------------------------
// bulkApproveStatements
// ---------------------------------------------------------------------

describe('bulkApproveStatements', () => {
  it('approves multiple statements and reports per-id outcomes', async () => {
    const state: State = {
      statements: [
        {
          id: 1,
          supplier_code: 'A',
          status: 'queued',
          statement_date: null,
          received_date: null,
          sender_email: 'a@a.com',
          acknowledged_at: null,
          response_text: 'ok',
          response_subject: null,
          email_pdf_path: null,
        },
        {
          id: 2,
          supplier_code: 'B',
          status: 'sent',
          statement_date: null,
          received_date: null,
          sender_email: 'b@b.com',
          acknowledged_at: null,
          response_text: null,
          response_subject: null,
          email_pdf_path: null,
        },
      ],
      lines: [],
      contacts: [],
      automationSettings: {},
      comms: [],
    };
    const result = await bulkApproveStatements(
      makeAppDb(state),
      makeEmail(),
      supplierLookup,
      { statementIds: [1, 2], approvedBy: 'admin' },
    );
    expect(result.approved).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.results[0]?.success).toBe(true);
    expect(result.results[1]?.success).toBe(false);
  });
});
