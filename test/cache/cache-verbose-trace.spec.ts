import { describe, expect, it, vi } from 'vitest';
import { Logger } from '../../src/shared/logger';
import {
  createCacheQueryTracingProxy,
  runWithCacheVerboseTrace,
} from '../../src/shared/cache-verbose-trace';

describe('cache verbose query trace', () => {
  it('logs query shape and result count only inside a cache reload scope', async () => {
    const verbose = vi.spyOn(Logger.prototype, 'verbose').mockImplementation(() => {});
    const queryBuilder = createCacheQueryTracingProxy({
      getDatabaseType: () => 'postgres',
      find: vi.fn(async () => ({ data: [{ id: 1 }], count: 1 })),
    });

    await queryBuilder.find({
      table: 'outside',
      filter: { id: { _eq: 'not-logged' } },
    });
    expect(verbose).not.toHaveBeenCalled();

    await runWithCacheVerboseTrace(
      {
        cacheName: 'RouteCache',
        cacheIdentifier: 'route',
        reloadType: 'full',
      },
      () =>
        queryBuilder.find({
          table: 'enfyra_route',
          fields: ['id', 'methodConfigs.id'],
          filter: {
            _and: [
              { isEnabled: { _eq: true } },
              { secret: { _eq: 'must-not-appear' } },
            ],
          },
          sort: ['id'],
          limit: 50,
        }),
    );

    expect(verbose).toHaveBeenCalledTimes(1);
    const message = String(verbose.mock.calls[0][0]);
    expect(message).toContain('cache=RouteCache');
    expect(message).toContain('backend=postgres');
    expect(message).toContain('operation=find');
    expect(message).toContain('table=enfyra_route');
    expect(message).toContain('fields=[id,methodConfigs.id]');
    expect(message).toContain('filter={_and:[{isEnabled:{_eq:?}},{secret:{_eq:?}}]}');
    expect(message).toContain('rows=1');
    expect(message).not.toContain('must-not-appear');
    expect(message).not.toContain('not-logged');
  });
});
