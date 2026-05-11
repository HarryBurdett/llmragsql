/**
 * Reads /Users/maccb/llmragsql/systems.json and config.ini for credentials.
 * Same files the legacy Python uses — we read the SAME settings so the
 * test runner connects to the same Opera and same mailbox as legacy.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const SQLRAG_ROOT = '/Users/maccb/llmragsql';

interface SystemRecord {
  id: string;
  name: string;
  is_default: boolean;
  database: {
    type: string;
    server: string;
    port: string;
    database: string;
    username: string;
    password: string;
    trust_server_certificate?: string;
  };
}

export interface OperaSEConfig {
  systemName: string;
  server: string;
  port: number;
  username: string;
  password: string;
  /** The database name from the default system — typically a single company's DB. */
  defaultDatabase: string;
  trustServerCertificate: boolean;
}

export function readOperaSEConfig(): OperaSEConfig {
  const path = join(SQLRAG_ROOT, 'systems.json');
  const records: SystemRecord[] = JSON.parse(readFileSync(path, 'utf-8'));
  const def = records.find((r) => r.is_default);
  if (!def) throw new Error('No default system in systems.json');
  const d = def.database;
  return {
    systemName: def.name,
    server: d.server,
    port: parseInt(d.port, 10),
    username: d.username,
    password: d.password,
    defaultDatabase: d.database,
    trustServerCertificate: (d.trust_server_certificate ?? 'true') === 'true',
  };
}
