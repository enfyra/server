import { describe, expect, it } from 'vitest';
import type { TDynamicContext } from '../../src/shared/types';
import { deferDynamicTransactionEffect, runWithDeferredDynamicTransactionEffects } from '../../src/shared/utils/dynamic-transaction-effects.util';

describe('transaction effect attempts', () => {
  it('flushes only the committed Mongo attempt', async () => {
    const ctx = {} as TDynamicContext;
    const emitted: number[] = [];
    let attempt = 0;
    await runWithDeferredDynamicTransactionEffects(ctx, async () => {
      const current = ++attempt;
      deferDynamicTransactionEffect(ctx, () => { emitted.push(current); });
      return current;
    }, async (work) => {
      await work();
      expect(emitted).toEqual([]);
      return work();
    });
    expect(emitted).toEqual([2]);
  });

  it('discards a committed nested scope when its outer transaction rolls back', async () => {
    const ctx = {} as TDynamicContext;
    const emitted: string[] = [];
    await expect(runWithDeferredDynamicTransactionEffects(ctx, async () => {
      await runWithDeferredDynamicTransactionEffects(ctx, async () => {
        deferDynamicTransactionEffect(ctx, () => { emitted.push('update'); });
      }, (work) => work());
      throw new Error('rollback');
    })).rejects.toThrow('rollback');
    expect(emitted).toEqual([]);
  });
});
