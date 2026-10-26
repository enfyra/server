import { describe, expect, it } from 'vitest';
import type { TDynamicContext } from '../../src/shared/types';
import { deferDynamicTransactionEffect, deferDynamicTransactionResource, runWithDeferredDynamicTransactionEffects } from '../../src/shared/utils/dynamic-transaction-effects.util';

describe('transaction effect attempts', () => {
  it.each([false, true])('owns resource compensation separately from postcommit failure (rollback=%s)', async rollback => {
    const ctx = {} as TDynamicContext;
    const events: string[] = [];
    const result = runWithDeferredDynamicTransactionEffects(ctx, async () => {
      deferDynamicTransactionResource(() => { events.push('resource-commit'); }, () => { events.push('resource-rollback'); });
      deferDynamicTransactionEffect(ctx, () => { throw new Error('publication failed'); });
      if (rollback) throw new Error('rollback');
    });
    await expect(result).rejects.toThrow(rollback ? 'rollback' : 'publication failed');
    expect(events).toEqual([rollback ? 'resource-rollback' : 'resource-commit']);
  });

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

  it('does not compensate an abandoned resource while a native retry can still reuse it', async () => {
    const ctx = {} as TDynamicContext;
    const events: string[] = [];
    let attempt = 0;
    await runWithDeferredDynamicTransactionEffects(ctx, async () => {
      const current = ++attempt;
      expect(events).toEqual([]);
      deferDynamicTransactionResource(
        () => { events.push(`commit-${current}`); },
        () => { events.push(`cleanup-${current}`); },
      );
    }, async work => {
      await work();
      await work();
      expect(events).toEqual([]);
    });
    expect(events).toEqual(['cleanup-1', 'commit-2']);
  });

  it('compensates every attempted resource only after final rollback', async () => {
    const ctx = {} as TDynamicContext;
    const events: number[] = [];
    let attempt = 0;
    await expect(runWithDeferredDynamicTransactionEffects(ctx, async () => {
      const current = ++attempt;
      expect(events).toEqual([]);
      deferDynamicTransactionResource(() => {}, () => { events.push(current); });
    }, async work => {
      await work();
      await work();
      throw new Error('final rollback');
    })).rejects.toThrow('final rollback');
    expect(events).toEqual([2, 1]);
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
