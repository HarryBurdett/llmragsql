/**
 * Plugin context shape — must match SAM's AppContext defined at
 * ~/opera-knowledge-ref/packages/backend/src/plugins/context.ts
 *
 * We define our own minimal copy here to avoid coupling our build to
 * SAM's package layout. SAM's loader checks the factory signature
 * structurally, not by type.
 *
 * If SAM extends AppContext later (new fields), they're additive — our
 * plugin just ignores them. If SAM removes a field we depend on,
 * that's a breaking change SAM will announce.
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
  /** Opera database type for this tenant: 'opera-se' (SQL) or 'opera-3' (FoxPro) */
  operaType: OperaType | null;
  db: {
    sam: Knex;
    operaSystem: Knex | null;
    getCompanyDb: (code: string) => Knex | null;
  };
  eventBus: ScopedEventBus;
  logger: AppLogger;
}

/**
 * SAM's plugin loader expects `req.operaCompany` to be populated by its
 * own `resolveCompany` middleware before our router runs. We declare
 * the augmentation here so TypeScript knows about it.
 */
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
