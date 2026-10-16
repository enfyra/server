import type { TDynamicContext } from '../types';
import { AsyncLocalStorage } from 'node:async_hooks';
import type { DynamicTransactionScopeRunner } from '../types/dynamic-transaction.types';

type DynamicTransactionEffect = () => void | Promise<void>;

const effectScopes = new AsyncLocalStorage<{
  ctx: TDynamicContext;
  effects: DynamicTransactionEffect[];
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
  const attempt = () => {
    effects = [];
    return effectScopes.run({ ctx, effects }, callback);
  };
  const result = await (runTransaction ? runTransaction(attempt) : attempt());
  if (parent?.ctx === ctx) {
    parent.effects.push(...effects);
    return result;
  }
  let firstError: unknown;
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
