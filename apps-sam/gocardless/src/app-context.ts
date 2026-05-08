/**
 * Plugin context shape — same shared definition pattern as
 * balance-check. See balance-check/src/app-context.ts for context.
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

/**
 * GoCardless declares `separateDatabase: true` in manifest, so SAM
 * provisions a per-app MSSQL database (`ai_sam_app_gocardless`) and
 * exposes it via `ctx.db.app` (Knex pool).
 */
export interface AppContext {
  appId: string;
  tenantId: string;
  config: Record<string, unknown>;
  operaType: OperaType | null;
  db: {
    sam: Knex;
    app: Knex | null; // per-app DB (mandate registry, payment requests, etc.)
    operaSystem: Knex | null;
    getCompanyDb: (code: string) => Knex | null;
  };
  eventBus: ScopedEventBus;
  logger: AppLogger;
  /** SAM-provided email functions (inbox + send). Optional in test contexts. */
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
      user?: {
        userId: string;
        email: string;
        role: string;
        tenantId: string;
        userType: string;
        permissions: string[];
        appId?: string;
        appRole?: string | null;
      };
    }
  }
}

export type AppBackendFactory = (context: AppContext) => import('express').Router;
