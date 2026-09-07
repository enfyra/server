import { createHash, randomUUID } from 'node:crypto';
import { logStore } from './log-store';
import type { PendingRuntimeLog, RuntimeLogRecord, RuntimeLogTable } from './types/runtime-log.types';

const MAX_RECORD_BYTES = 65_536;
const MAX_QUEUE_BYTES = 8 * 1024 * 1024;
const pending: PendingRuntimeLog[] = [];
const capturedUserLogs = new WeakSet<unknown[]>();
let queuedBytes = 0;
let dropped = 0;
let instanceId: string | null = null;
let consoleCaptureInstalled = false;
let flushCallback: (() => Promise<void>) | undefined;
const sensitive = /authorization|cookie|password|secret|token|credential|api[-_]?key/i;

export function sanitizeRuntimeLog(value: unknown): unknown {
  const seen = new WeakSet<object>();
  let nodes = 0;
  let characters = 16_000;
  const visit = (item: any, depth: number): unknown => {
    if (++nodes > 2000 || depth > 8) return '[Truncated]';
    if (typeof item === 'string') {
      const text = item.slice(0, Math.max(0, Math.min(4096, characters)));
      characters -= text.length;
      return text
      .replace(/(Bearer\s+)[^\s"']+/gi, '$1[REDACTED]')
      .replace(/((?:password|secret|token|api[_-]?key)\s*[=:]\s*)[^\s&,;]+/gi, '$1[REDACTED]')
      .replace(/(\w+:\/\/)[^\s/@:]+:[^\s/@]+@/g, '$1[REDACTED]@');
    }
    if (item === null || typeof item === 'number' || typeof item === 'boolean') return item;
    if (typeof item === 'bigint') return String(item);
    if (typeof item !== 'object') return String(item);
    if (seen.has(item)) return '[Circular]';
    seen.add(item);
    if (item instanceof Error) return visit({ name: item.name, message: item.message, stack: item.stack, code: (item as any).code }, depth + 1);
    if (item instanceof Date) return item.toISOString();
    if (Array.isArray(item)) return item.slice(0, 200).map((entry) => visit(entry, depth + 1));
    return Object.fromEntries(Object.entries(item).slice(0, 100).map(([key, entry]) => [key, sensitive.test(key) ? '[REDACTED]' : visit(entry, depth + 1)]));
  };
  try { return visit(value, 0); } catch { return '[Unserializable]'; }
}

function enqueue(table: RuntimeLogTable, record: RuntimeLogRecord): void {
  const bytes = Buffer.byteLength(JSON.stringify(record));
  if (bytes > MAX_RECORD_BYTES || pending.length >= 2000 || queuedBytes + bytes > MAX_QUEUE_BYTES) {
    dropped++;
    if (dropped === 1 || dropped % 100 === 0) process.stderr.write('[RuntimeLog] Memory buffer full; log records dropped\n');
    return;
  }
  pending.push({ table, record, bytes });
  queuedBytes += bytes;
}

function baseRecord(component: string, metadata: any = {}): RuntimeLogRecord {
  return {
    eventId: randomUUID(), occurredAt: new Date().toISOString(),
    correlationId: metadata.correlationId == null ? logStore.getStore()?.correlationId ?? null : String(metadata.correlationId).slice(0, 255),
    instanceId, component: component.slice(0, 255),
    sourceKind: metadata.sourceKind == null ? null : String(metadata.sourceKind).slice(0, 255), sourceId: metadata.sourceId == null ? null : String(metadata.sourceId).slice(0, 255),
    statusCode: Number.isInteger(metadata.statusCode) ? metadata.statusCode : null,
  };
}

export function recordSystemError(message: string, metadata: any = {}): void {
  const safe = sanitizeRuntimeLog(metadata) as any;
  const safeMessage = String(sanitizeRuntimeLog(message));
  const code = String(metadata.code ?? metadata.errorCode ?? metadata.data?.event ?? 'SYSTEM_ERROR').slice(0, 255);
  const component = String(metadata.context ?? metadata.component ?? 'System');
  const details = JSON.stringify(safe);
  enqueue('enfyra_system_error', {
    ...baseRecord(component, metadata), code, message: safeMessage,
    severity: metadata.fatal ? 'fatal' : 'error',
    fingerprint: createHash('sha256').update(component + ':' + code + ':' + safeMessage).digest('hex'),
    stack: typeof safe.stack === 'string' ? safe.stack : null,
    details: Buffer.byteLength(details) > 24_000 ? { truncated: true, preview: Buffer.from(details).subarray(0, 20_000).toString('utf8') } : safe,
  });
}

export function recordUserLog(entries: unknown[], metadata: any = {}): void {
  if (!entries.length || capturedUserLogs.has(entries)) return;
  capturedUserLogs.add(entries);
  const selected: unknown[] = [];
  let bytes = 0;
  for (const entry of entries.slice(0, 200)) {
    const safe = sanitizeRuntimeLog(entry);
    const size = Buffer.byteLength(JSON.stringify(safe));
    if (bytes + size > 40_000) break;
    selected.push(safe); bytes += size;
  }
  enqueue('enfyra_user_log', {
    ...baseRecord(String(metadata.component ?? 'Script'), metadata),
    entries: selected, entryCount: entries.length,
    truncated: selected.length < entries.length || metadata.truncated === true,
  });
}

export function peekRuntimeLogs(limit = 50): PendingRuntimeLog[] { return pending.slice(0, limit); }
export function acknowledgeRuntimeLog(eventId: string): void {
  const index = pending.findIndex((item) => item.record.eventId === eventId);
  if (index < 0) return;
  queuedBytes -= pending[index].bytes;
  pending.splice(index, 1);
}
export function setRuntimeLogInstance(value: string): void { instanceId = value; }
export function getRuntimeLogBufferStats() { return { pending: pending.length, bytes: queuedBytes, dropped }; }
export function setRuntimeLogFlush(callback: (() => Promise<void>) | undefined): void { flushCallback = callback; }
export async function flushRuntimeLogsBeforeExit(): Promise<void> {
  if (!flushCallback) return;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([flushCallback(), new Promise<void>((resolve) => { timer = setTimeout(resolve, 1500); })]);
  } finally { clearTimeout(timer); }
}

export function installConsoleErrorCapture(): void {
  if (consoleCaptureInstalled) return;
  consoleCaptureInstalled = true;
  const original = console.error.bind(console);
  console.error = (...args: unknown[]) => {
    recordSystemError(typeof args[0] === 'string' ? args[0] : 'Console error', { context: 'Console', arguments: args });
    original(...args.map(sanitizeRuntimeLog));
  };
}
