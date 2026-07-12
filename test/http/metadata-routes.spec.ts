import { describe, expect, it, vi } from 'vitest';
import { registerMetadataRoutes } from '../../src/http/routes/metadata.routes';

function registerRoutes() {
  const handlers = new Map<string, (req: any, res: any) => unknown>();
  const app = {
    get(path: string, handler: (req: any, res: any) => unknown) {
      handlers.set(path, handler);
    },
  };
  const table = { name: 'posts', columns: [], relations: [] };
  const container = {
    cradle: {
      databaseConfigService: { getDbType: () => 'postgres' },
      runtimeRegistryService: {
        getMetadata: () => ({
          tablesList: [table],
          tables: new Map([[table.name, table]]),
        }),
        getRoutes: () => [],
      },
      policyService: { checkRequestAccess: () => ({ allow: true }) },
    },
  };

  registerMetadataRoutes(app as any, container as any);
  return handlers;
}

describe('metadata routes', () => {
  it('returns only projected table metadata from GET /metadata/:name', async () => {
    const handler = registerRoutes().get('/metadata/:name')!;
    const json = vi.fn();

    await handler(
      { params: { name: 'posts' }, user: { isRootAdmin: true } },
      { json },
    );

    expect(json).toHaveBeenCalledWith({
      data: expect.objectContaining({ name: 'posts' }),
    });
    expect(json.mock.calls[0][0]).not.toHaveProperty('dbType');
    expect(json.mock.calls[0][0]).not.toHaveProperty('enfyraVersion');
  });
});
