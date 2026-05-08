/**
 * Plugin context shape — same shared definition pattern as
 * the other apps. See balance-check/src/app-context.ts for context.
 */
import type { Knex } from 'knex';

export type OperaType = 'opera-se' | 'opera-3';

export interface ScopedEventBus {
  emit: (eventType: string, data: unknown) => void;
  on: (eventType: string, handler: (data: unknown) => void | Promise<void>) => () => void;
}

export interface AppLogger {
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
  debug(message: string, ...args: unknown[]): void;
}

export interface AppContext {
  appId: string;
  tenantId: string;
  config: Record<string, unknown>;
  operaType: OperaType | null;
  db: {
    sam: Knex;
    app: Knex | null; // supplier_statements + extraction cache + automation rules
    operaSystem: Knex | null;
    getCompanyDb: (code: string) => Knex | null;
  };
  eventBus: ScopedEventBus;
  logger: AppLogger;
  email?: {
    send(opts: {
      to: string | string[];
      cc?: string | string[];
      bcc?: string | string[];
      subject: string;
      bodyHtml?: string;
      bodyText?: string;
      attachments?: unknown[];
    }): Promise<{ success: boolean; error?: string }>;
    isConfigured(): boolean;
  };
}

declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace Express {
    interface Request {
      operaCompany?: string;
    }
  }
}

export type AppBackendFactory = (context: AppContext) => import('express').Router;
