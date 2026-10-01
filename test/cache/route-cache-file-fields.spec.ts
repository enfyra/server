import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { RouteCacheService } from '../../src/engines/cache/services/route-cache.service';

describe('route cache file-field changes', () => {
  it('reloads full route data when a child file field changes or is deleted', async () => {
    const cache = new RouteCacheService({
      queryBuilderService: {} as any,
      metadataCacheService: {} as any,
      eventEmitter: new EventEmitter2(),
    });
    const reload = vi.spyOn(cache, 'reload').mockResolvedValue(undefined);
    await cache.partialReload({ table: 'enfyra_route_method_config_file_field', action: 'delete', scope: 'partial', ids: [5], timestamp: Date.now() } as any, false);
    expect(reload).toHaveBeenCalledOnce();
  });
});
