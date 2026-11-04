import { EventEmitter2 } from 'eventemitter2';
import { afterEach, expect, it, vi } from 'vitest';
import { GuardCacheBuilder } from '../../src/engines/cache/services/guard-cache-builder.service';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

afterEach(() => DatabaseConfigService.resetForTesting());
it('refreshes moved/deleted guard rules without reading every guard definition', async () => {
  DatabaseConfigService.overrideForTesting('postgres');
  let owner = 1;
  let enabled = true;
  const find = vi.fn(async (query: any) => ({
    data:
      query.table === 'enfyra_guard'
        ? [1, 2].map((id) => ({
            id,
            name: `guard${id}`,
            position: 'pre_auth',
            isEnabled: true,
            isGlobal: true,
            methods: [],
          }))
        : enabled
          ? [
              {
                id: 10,
                guard: { id: owner },
                type: 'ip_whitelist',
                config: { ips: ['127.0.0.1'] },
                users: [],
                isEnabled: true,
              },
            ]
          : [],
  }));
  const cache = new GuardCacheBuilder({
    eventEmitter: new EventEmitter2(),
    queryBuilderService: { find, isMongoDb: () => false } as any,
  });
  await cache.reload(false);
  owner = 2;
  find.mockClear();
  await cache.partialReload(
    {
      table: 'enfyra_guard_rule',
      ids: [10],
      scope: 'partial',
      action: 'reload',
      timestamp: 0,
    },
    false,
  );
  expect(
    find.mock.calls.every(
      ([query]) => query.table === 'enfyra_guard_rule' && query.filter.id,
    ),
  ).toBe(true);
  expect(
    cache.getRawCache().preAuthGlobal.map((guard) => guard.rules.length),
  ).toEqual([0, 1]);
  enabled = false;
  await cache.partialReload(
    {
      table: 'enfyra_guard_rule',
      ids: [10],
      scope: 'partial',
      action: 'reload',
      timestamp: 0,
    },
    false,
  );
  expect(
    cache
      .getRawCache()
      .preAuthGlobal.every((guard) => guard.rules.length === 0),
  ).toBe(true);
});
