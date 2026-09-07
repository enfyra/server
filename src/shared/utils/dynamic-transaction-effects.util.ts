import type { TDynamicContext } from '../types';

type DynamicTransactionEffect = () => void | Promise<void>;

const effectScopes = new WeakMap<TDynamicContext, DynamicTransactionEffect[]>();

export function deferDynamicTransactionEffect(
  ctx: TDynamicContext,
  effect: DynamicTransactionEffect,
): boolean {
  const effects = effectScopes.get(ctx);
  if (!effects) return false;
  effects.push(effect);
  return true;
}

export async function runWithDeferredDynamicTransactionEffects<T>(
  ctx: TDynamicContext,
  callback: () => Promise<T>,
): Promise<T> {
  if (effectScopes.has(ctx)) return callback();

  const effects: DynamicTransactionEffect[] = [];
  effectScopes.set(ctx, effects);
  let result: T;
  try {
    result = await callback();
  } catch (error) {
    effectScopes.delete(ctx);
    throw error;
  }

  effectScopes.delete(ctx);
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
