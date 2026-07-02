import { AsyncLocalStorage } from 'async_hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

let activeAbortSignal: AbortSignal | undefined;

vi.mock('@enfyra/kernel', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@enfyra/kernel')>();
  return {
    ...actual,
    getIoAbortSignal: () => activeAbortSignal,
  };
});

import { KnexHookManagerService } from '../../src/engines/knex/services/knex-hook-manager.service';

function createService() {
  return new KnexHookManagerService({
    runtimeRegistryService: {
      getTableMetadata: vi.fn(),
    } as any,
  });
}

function createQueryBuilder(result: any, beforeResolve?: () => void) {
  const originalThen = vi.fn(
    (onFulfilled: (value: any) => any, onRejected?: any) => {
      beforeResolve?.();
      return Promise.resolve(result).then(onFulfilled, onRejected);
    },
  );
  return {
    _single: { table: 'example_table' },
    insert: vi.fn(),
    update: vi.fn(),
    delete: vi.fn(),
    del: vi.fn(),
    then: originalThen,
    originalThen,
  };
}

describe('KnexHookManagerService select query wrapping', () => {
  beforeEach(() => {
    activeAbortSignal = undefined;
    vi.clearAllMocks();
  });

  it('awaits beforeSelect hooks before executing the query', async () => {
    const service = createService();
    const queryBuilder = createQueryBuilder([{ id: 1 }]);
    const events: string[] = [];

    service.addHook('beforeSelect', async () => {
      events.push('before-start');
      await Promise.resolve();
      events.push('before-end');
    });
    service.addHook('afterSelect', async (_tableName, result) => {
      events.push('after');
      return result;
    });

    const wrapped = service.wrapQueryBuilder(
      queryBuilder,
      {} as any,
      () => ({}) as any,
      new AsyncLocalStorage(),
      new AsyncLocalStorage(),
    );

    await wrapped.then((result: any) => {
      events.push(`result:${result[0].id}`);
      return result;
    });

    expect(events).toEqual([
      'before-start',
      'before-end',
      'after',
      'result:1',
    ]);
    expect(queryBuilder.originalThen).toHaveBeenCalledTimes(1);
  });

  it('routes beforeSelect failures through the returned promise', async () => {
    const service = createService();
    const queryBuilder = createQueryBuilder([{ id: 1 }]);

    service.addHook('beforeSelect', async () => {
      throw new Error('before failed');
    });

    const wrapped = service.wrapQueryBuilder(
      queryBuilder,
      {} as any,
      () => ({}) as any,
      new AsyncLocalStorage(),
      new AsyncLocalStorage(),
    );

    await expect(wrapped.then()).rejects.toThrow('before failed');
    expect(queryBuilder.originalThen).not.toHaveBeenCalled();
  });

  it('does not reject when the request aborts after the database returns', async () => {
    const service = createService();
    const abortController = new AbortController();
    activeAbortSignal = abortController.signal;
    const queryBuilder = createQueryBuilder([{ id: 1 }], () => {
      abortController.abort();
    });

    const afterSelect = vi.fn(async (_tableName, result) => result);
    service.addHook('afterSelect', afterSelect);

    const wrapped = service.wrapQueryBuilder(
      queryBuilder,
      {} as any,
      () => ({}) as any,
      new AsyncLocalStorage(),
      new AsyncLocalStorage(),
    );

    await expect(wrapped.then()).resolves.toEqual([{ id: 1 }]);
    expect(afterSelect).not.toHaveBeenCalled();
  });
});
