import { describe, it, expect, vi, afterEach } from 'vitest';
import { WebsocketCacheBuilder } from '../../src/engines/cache';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

afterEach(() => DatabaseConfigService.resetForTesting());

describe('WebSocket cache backend identity projections', () => {
  it.each(['postgres', 'mysql', 'mongodb'] as const)('loads and resolves events using %s identity only', async (dbType) => {
    DatabaseConfigService.overrideForTesting(dbType);
    const pk = dbType === 'mongodb' ? '_id' : 'id';
    const query = { isMongoDb: () => dbType === 'mongodb', find: vi.fn(async () => ({ data: [] })) };
    const cache = new WebsocketCacheBuilder({ queryBuilderService: query as any });
    await (cache as any).attachEvents([{ [pk]: 1 }]);
    await (cache as any).resolveGatewayIdsForEvents([1]);
    expect(query.find.mock.calls[0][0].fields).toEqual(['*', `gateway.${pk}`]);
    expect(query.find.mock.calls[1][0].fields).toEqual([`gateway.${pk}`]);
  });
});
