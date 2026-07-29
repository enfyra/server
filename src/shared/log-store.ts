import { AsyncLocalStorage } from 'async_hooks';

export interface LogStore {
  correlationId?: string;
  context?: Record<string, any>;
}

export const logStore = new AsyncLocalStorage<LogStore>();

export function setCorrelationId(correlationId: string): void {
  const s = logStore.getStore();
  if (s) s.correlationId = correlationId;
}

export function mergeLogContext(ctx: Record<string, any>): void {
  const s = logStore.getStore();
  if (s) s.context = { ...(s.context || {}), ...ctx };
}

export function clearLogContext(): void {
  const s = logStore.getStore();
  if (s) {
    s.correlationId = undefined;
    s.context = {};
  }
}
