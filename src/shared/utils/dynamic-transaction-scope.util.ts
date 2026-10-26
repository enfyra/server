import { AsyncLocalStorage } from 'node:async_hooks';
import type { TDynamicContext } from '../types';

const transactionContexts = new AsyncLocalStorage<TDynamicContext>();

export function runWithDynamicTransactionScope<T>(
  ctx: TDynamicContext,
  work: () => Promise<T>,
): Promise<T> {
  return transactionContexts.run(ctx, work);
}

export function isInDynamicTransactionScope(ctx: TDynamicContext): boolean {
  return transactionContexts.getStore() === ctx;
}
