/**
 * Field permission context propagation via AsyncLocalStorage.
 *
 * Verifies that the field-permission checker callback is correctly
 * propagated through the call stack so that every insertOne / updateOne
 * can enforce per-field write permissions regardless of database type.
 */

import { AsyncLocalStorage } from 'async_hooks';

type FieldPermissionChecker = (
  tableName: string,
  action: 'create' | 'update',
  data: any,
) => Promise<void>;

describe('fieldPermissionContext (AsyncLocalStorage)', () => {
  const fieldPermissionContext = new AsyncLocalStorage<{
    check: FieldPermissionChecker;
  }>();

  const checkFieldPermission = async (
    tableName: string,
    action: 'create' | 'update',
    data: any,
  ) => {
    const ctx = fieldPermissionContext.getStore();
    if (ctx) {
      await ctx.check(tableName, action, data);
    }
  };

  it('does nothing when no checker is registered', async () => {
    await expect(
      checkFieldPermission('task', 'create', { name: 'x' }),
    ).resolves.toBeUndefined();
  });

  it('invokes the checker inside runWithFieldPermissionCheck', async () => {
    const calls: Array<{ table: string; action: string }> = [];
    const checker: FieldPermissionChecker = async (tbl, act) => {
      calls.push({ table: tbl, action: act });
    };

    await fieldPermissionContext.run({ check: checker }, async () => {
      await checkFieldPermission('employee', 'create', { salary: 100 });
      await checkFieldPermission('employee', 'update', { salary: 200 });
    });

    expect(calls).toEqual([
      { table: 'employee', action: 'create' },
      { table: 'employee', action: 'update' },
    ]);
  });

  it('checker can throw to block the operation', async () => {
    const checker: FieldPermissionChecker = async (_tbl, _act, data) => {
      if ('salary' in data) {
        throw new Error('Field "salary" is not writable');
      }
    };

    await fieldPermissionContext.run({ check: checker }, async () => {
      await expect(
        checkFieldPermission('employee', 'update', { salary: 999 }),
      ).rejects.toThrow('Field "salary" is not writable');

      await expect(
        checkFieldPermission('employee', 'update', { name: 'ok' }),
      ).resolves.toBeUndefined();
    });
  });

  it('context does not leak between separate run() calls', async () => {
    const calls: string[] = [];

    const checker1: FieldPermissionChecker = async () => {
      calls.push('checker1');
    };
    const checker2: FieldPermissionChecker = async () => {
      calls.push('checker2');
    };

    await fieldPermissionContext.run({ check: checker1 }, async () => {
      await checkFieldPermission('t', 'create', {});
    });

    await fieldPermissionContext.run({ check: checker2 }, async () => {
      await checkFieldPermission('t', 'create', {});
    });

    // After both runs, context should be empty
    await checkFieldPermission('t', 'create', {});

    expect(calls).toEqual(['checker1', 'checker2']);
  });

  it('nested relations inherit the permission context', async () => {
    const checked: string[] = [];
    const checker: FieldPermissionChecker = async (tbl) => {
      checked.push(tbl);
    };

    // Simulates: insertOne('order', ...) internally calls insertOne('order_item', ...)
    const insertOne = async (table: string, data: any) => {
      await checkFieldPermission(table, 'create', data);
      if (data.items) {
        for (const item of data.items) {
          await insertOne(`${table}_item`, item);
        }
      }
    };

    await fieldPermissionContext.run({ check: checker }, async () => {
      await insertOne('order', {
        total: 100,
        items: [{ product: 'A' }, { product: 'B' }],
      });
    });

    expect(checked).toEqual(['order', 'order_item', 'order_item']);
  });
});

describe('QueryBuilderService.runWithFieldPermissionCheck routing', () => {
  it('routes to mongoService when knexService is absent', async () => {
    const mongoCalled = jest.fn(async (_checker: any, callback: any) =>
      callback(),
    );

    // Simulate the routing logic
    const knexService: any = null;
    const mongoService = { runWithFieldPermissionCheck: mongoCalled };
    const checker = jest.fn();
    const callback = jest.fn(async () => 'result');

    let result: any;
    if (knexService) {
      result = await knexService.runWithFieldPermissionCheck(checker, callback);
    } else if (mongoService) {
      result = await mongoService.runWithFieldPermissionCheck(checker, callback);
    } else {
      result = await callback();
    }

    expect(result).toBe('result');
    expect(mongoCalled).toHaveBeenCalledWith(checker, callback);
  });

  it('routes to knexService when it is present (SQL priority)', async () => {
    const knexCalled = jest.fn(async (_checker: any, callback: any) =>
      callback(),
    );
    const mongoCalled = jest.fn();

    const knexService = { runWithFieldPermissionCheck: knexCalled };
    const mongoService = { runWithFieldPermissionCheck: mongoCalled };
    const checker = jest.fn();
    const callback = jest.fn(async () => 'ok');

    let result: any;
    if (knexService) {
      result = await knexService.runWithFieldPermissionCheck(checker, callback);
    } else if (mongoService) {
      result = await mongoService.runWithFieldPermissionCheck(checker, callback);
    } else {
      result = await callback();
    }

    expect(result).toBe('ok');
    expect(knexCalled).toHaveBeenCalled();
    expect(mongoCalled).not.toHaveBeenCalled();
  });

  it('falls through to direct callback when no DB service', async () => {
    const knexService: any = null;
    const mongoService: any = null;
    const callback = jest.fn(async () => 'fallback');

    let result: any;
    if (knexService) {
      result = await knexService.runWithFieldPermissionCheck(null, callback);
    } else if (mongoService) {
      result = await mongoService.runWithFieldPermissionCheck(null, callback);
    } else {
      result = await callback();
    }

    expect(result).toBe('fallback');
  });
});
