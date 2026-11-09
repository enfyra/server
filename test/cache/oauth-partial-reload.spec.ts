import { afterEach, expect, it, vi } from 'vitest';
import { OAuthConfigCacheBuilder } from '../../src/engines/cache/services/oauth-config-cache-builder.service';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

afterEach(() => DatabaseConfigService.resetForTesting());
it('removes a disabled OAuth provider while preserving an untouched provider', async () => {
  DatabaseConfigService.overrideForTesting('postgres');
  const rows = [
    { id: 1, provider: 'google', isEnabled: true },
    { id: 2, provider: 'github', isEnabled: true },
  ];
  const find = vi.fn(async (query: any) => ({
    data: rows.filter(
      (row) =>
        row.isEnabled &&
        (!query.filter._and || query.filter._and[1].id._in.includes(row.id)),
    ),
  }));
  const cache = new OAuthConfigCacheBuilder({
    queryBuilderService: { find } as any,
  });
  await cache.reload(false);
  const retained = cache.getRawCache().get('github');
  rows[0].isEnabled = false;
  find.mockClear();
  await cache.partialReload(
    {
      table: 'enfyra_oauth_config',
      ids: [1],
      scope: 'partial',
      action: 'reload',
      timestamp: 0,
    },
    false,
  );
  expect(cache.getRawCache().has('google')).toBe(false);
  expect(cache.getRawCache().get('github')).toBe(retained);
  expect(find.mock.calls[0][0].filter._and[1]).toEqual({ id: { _in: [1] } });
});
