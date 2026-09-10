import { AsyncLocalStorage } from 'async_hooks';
import { describe, expect, it, vi } from 'vitest';
import { KnexHookManagerService } from '../../src/engines/knex/services/knex-hook-manager.service';

function createService() {
  const tableMetadata = {
    name: 'ai_models',
    columns: [
      {
        name: 'id',
        type: 'int',
        isPrimary: true,
        isPublished: true,
        isUpdatable: false,
      },
      {
        name: 'upstreamModel',
        type: 'varchar',
        isNullable: true,
        isPublished: false,
        isUpdatable: true,
      },
    ],
    relations: [],
  };
  const metadata = {
    tables: new Map([['ai_models', tableMetadata]]),
  };
  const runtimeRegistryService = {
    getMetadata: vi.fn(() => metadata),
    getTableMetadata: vi.fn(() => tableMetadata),
  };
  const service = new KnexHookManagerService({
    runtimeRegistryService: runtimeRegistryService as any,
  });
  const knexContext = new AsyncLocalStorage<any>();
  const cascadeContext = new AsyncLocalStorage<Map<string, any>>();
  const policyContext = new AsyncLocalStorage<any>();
  const fieldPermissionContext = new AsyncLocalStorage<any>();
  const knex = { raw: vi.fn() } as any;

  service.initialize(
    'postgres',
    knex,
    knexContext,
    cascadeContext,
    policyContext,
    fieldPermissionContext,
    () => knex,
    (_tableName, data) => data,
    (_tableName, data) => data,
    vi.fn(),
    vi.fn(),
  );
  return service;
}

describe('KnexHookManagerService update normalization', () => {
  it('preserves an authorized null update for an unpublished nullable field', async () => {
    const service = createService();

    const result = await service.runHooks('beforeUpdate', 'ai_models', {
      id: 7,
      upstreamModel: null,
    });

    expect(result).toHaveProperty('upstreamModel', null);
  });
});
