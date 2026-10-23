import type { TDynamicContext } from '../types';
import { AsyncLocalStorage } from 'node:async_hooks';
import type { DynamicTransactionScopeRunner, DynamicTransactionResourceEffect as DynamicTransactionEffect } from '../types/dynamic-transaction.types';

const effectScopes = new AsyncLocalStorage<{
  ctx: TDynamicContext;
  effects: DynamicTransactionEffect[];
  rollback: DynamicTransactionEffect[];
}>();

export function deferDynamicTransactionEffect(
  ctx: TDynamicContext,
  effect: DynamicTransactionEffect,
): boolean {
  const scope = effectScopes.getStore();
  if (!scope || scope.ctx !== ctx) return false;
  scope.effects.push(effect);
  return true;
}

export function deferDynamicTransactionResource(
  commit: DynamicTransactionEffect,
  rollback: DynamicTransactionEffect,
): boolean {
  const scope = effectScopes.getStore();
  if (!scope) return false;
  scope.effects.push(commit);
  scope.rollback.push(rollback);
  return true;
}

export function bindDynamicTransactionEffects(ctx: TDynamicContext): DynamicTransactionScopeRunner {
  const scope = effectScopes.getStore();
  return (work) => scope?.ctx === ctx ? effectScopes.run(scope, work) : work();
}

export async function runWithDeferredDynamicTransactionEffects<T>(
  ctx: TDynamicContext,
  callback: () => Promise<T>,
  runTransaction?: (attempt: () => Promise<T>) => Promise<T>,
): Promise<T> {
  const parent = effectScopes.getStore();
  if (!runTransaction && parent?.ctx === ctx) return callback();
  let effects: DynamicTransactionEffect[] = [];
  let rollback: DynamicTransactionEffect[] = [];
  const abandoned: DynamicTransactionEffect[] = [];
  const compensate = async (pending: DynamicTransactionEffect[]) => {
    const failures: unknown[] = [];
    for (const effect of [...pending].reverse()) {
      try { await effect(); } catch (error) { failures.push(error); }
    }
    if (failures.length) throw new AggregateError(failures, 'Resource compensation failed');
  };
  const attempt = async () => {
    abandoned.push(...rollback);
    effects = [];
    rollback = [];
    return effectScopes.run({ ctx, effects, rollback }, callback);
  };
  let result: T;
  try {
    result = await (runTransaction ? runTransaction(attempt) : attempt());
  } catch (error) {
    try { await compensate([...abandoned, ...rollback]); } catch (cleanupError) {
      throw new AggregateError([error, cleanupError], 'Transaction failed and resource compensation failed');
    }
    throw error;
  }
  if (parent?.ctx === ctx) {
    parent.effects.push(() => compensate(abandoned), ...effects);
    parent.rollback.push(...abandoned, ...rollback);
    return result;
  }
  let firstError: unknown;
  try { await compensate(abandoned); } catch (error) { firstError = error; }
  for (const effect of effects) {
    try {
      await effect();
    } catch (error) {
      firstError ??= error;
    }
  }
  if (firstError) throw firstError;
  return result;
}
