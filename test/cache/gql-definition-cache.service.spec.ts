import { EventEmitter2 } from '@nestjs/event-emitter';
import { GqlDefinitionCacheService, TGqlDefinition } from '../../src/infrastructure/cache/services/gql-definition-cache.service';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

async function createCache(
  gqlRows: any[],
  tableRows?: any[],
): Promise<GqlDefinitionCacheService> {
  const find = jest.fn(async (params: any) => {
    if (params.table === 'gql_definition') return { data: gqlRows };
    if (params.table === 'table_definition') return { data: tableRows ?? [] };
    return { data: [] };
  });
  const qb = { find } as any;
  const ee = new EventEmitter2();
  const svc = new GqlDefinitionCacheService(qb, ee);
  await svc.reload(false);
  return svc;
}

function makeRow(
  overrides: Partial<{
    id: number;
    isEnabled: boolean;
    isSystem: boolean;
    description: string | null;
    metadata: Record<string, any> | null;
    tableName: string;
    tableId: number;
  }> = {},
): any {
  return {
    id: overrides.id ?? 1,
    isEnabled: overrides.isEnabled ?? true,
    isSystem: overrides.isSystem ?? false,
    description: overrides.description ?? null,
    metadata: overrides.metadata ?? null,
    table: { name: overrides.tableName ?? 'tasks' },
    tableId: overrides.tableId ?? 1,
  };
}

describe('GqlDefinitionCacheService', () => {
  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  describe('load() and transformData()', () => {
    it('loads enabled definitions into cache map', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({ id: 1, tableName: 'tasks', isEnabled: true }),
        makeRow({ id: 2, tableName: 'users', isEnabled: true }),
      ]);

      expect(await svc.isEnabledForTable('tasks')).toBe(true);
      expect(await svc.isEnabledForTable('users')).toBe(true);
      expect(await svc.isEnabledForTable('unknown')).toBe(false);
    });

    it('returns false for disabled definitions', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({ id: 1, tableName: 'tasks', isEnabled: false }),
      ]);

      expect(await svc.isEnabledForTable('tasks')).toBe(false);
    });

    it('skips rows with no table name', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        { id: 1, isEnabled: true, table: { name: 'tasks' } },
        { id: 2, isEnabled: true, table: null },
        { id: 3, isEnabled: true, table: { name: '' } },
        { id: 4, isEnabled: true, table: {} },
        { id: 5, isEnabled: true },
      ]);

      const all = await svc.getAllEnabled();
      expect(all).toHaveLength(1);
      expect(all[0].tableName).toBe('tasks');
    });

    it('handles empty database result gracefully', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([]);

      expect(await svc.isEnabledForTable('anything')).toBe(false);
      expect(await svc.getAllEnabled()).toEqual([]);
      expect(await svc.getForTable('anything')).toBeUndefined();
    });

    it('treats missing isEnabled as true (default)', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        { id: 1, table: { name: 'tasks' }, isSystem: false },
      ]);

      expect(await svc.isEnabledForTable('tasks')).toBe(true);
    });

    it('handles loadFromDb throwing an error', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const find = jest.fn().mockRejectedValue(new Error('db down'));
      const qb = { find } as any;
      const ee = new EventEmitter2();
      const svc = new GqlDefinitionCacheService(qb, ee);
      await svc.reload(false);

      expect(await svc.isEnabledForTable('tasks')).toBe(false);
      expect(await svc.getAllEnabled()).toEqual([]);
    });
  });

  describe('isEnabledForTable()', () => {
    it('returns true for enabled tables', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({ id: 1, tableName: 'posts', isEnabled: true }),
      ]);

      expect(await svc.isEnabledForTable('posts')).toBe(true);
    });

    it('returns false for unknown tables', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({ id: 1, tableName: 'posts', isEnabled: true }),
      ]);

      expect(await svc.isEnabledForTable('nonexistent')).toBe(false);
    });

    it('returns false for disabled tables', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({ id: 1, tableName: 'posts', isEnabled: false }),
      ]);

      expect(await svc.isEnabledForTable('posts')).toBe(false);
    });
  });

  describe('getForTable()', () => {
    it('returns full definition for known table', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({
          id: 5,
          tableName: 'orders',
          isEnabled: true,
          isSystem: true,
          description: 'Order table GQL',
          metadata: { maxDepth: 5 },
        }),
      ]);

      const def = await svc.getForTable('orders');
      expect(def).toEqual<TGqlDefinition>({
        id: 5,
        isEnabled: true,
        isSystem: true,
        description: 'Order table GQL',
        metadata: { maxDepth: 5 },
        tableName: 'orders',
      });
    });

    it('returns undefined for unknown table', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({ tableName: 'orders' }),
      ]);

      expect(await svc.getForTable('missing')).toBeUndefined();
    });
  });

  describe('getAllEnabled()', () => {
    it('returns only enabled definitions', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({ id: 1, tableName: 'tasks', isEnabled: true }),
        makeRow({ id: 2, tableName: 'users', isEnabled: false }),
        makeRow({ id: 3, tableName: 'orders', isEnabled: true }),
      ]);

      const enabled = await svc.getAllEnabled();
      expect(enabled).toHaveLength(2);
      const names = enabled.map((d) => d.tableName).sort();
      expect(names).toEqual(['orders', 'tasks']);
    });

    it('returns empty array when all are disabled', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({ id: 1, tableName: 'tasks', isEnabled: false }),
        makeRow({ id: 2, tableName: 'users', isEnabled: false }),
      ]);

      expect(await svc.getAllEnabled()).toEqual([]);
    });

    it('returns empty array for empty cache', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([]);
      expect(await svc.getAllEnabled()).toEqual([]);
    });
  });

  describe('manual table name resolution fallback', () => {
    it('resolves tableId to table name when join data is missing', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const gqlRows = [
        { id: 1, isEnabled: true, tableId: 10, table: null },
        { id: 2, isEnabled: true, tableId: 20, table: null },
      ];
      const tableRows = [
        { id: 10, name: 'tasks' },
        { id: 20, name: 'users' },
      ];

      const find = jest.fn(async (params: any) => {
        if (params.table === 'gql_definition') return { data: gqlRows };
        if (params.table === 'table_definition') return { data: tableRows };
        return { data: [] };
      });
      const qb = { find } as any;
      const ee = new EventEmitter2();
      const svc = new GqlDefinitionCacheService(qb, ee);
      await svc.reload(false);

      expect(await svc.isEnabledForTable('tasks')).toBe(true);
      expect(await svc.isEnabledForTable('users')).toBe(true);
    });

    it('handles MongoDB _id pk field for manual resolution', async () => {
      DatabaseConfigService.overrideForTesting('mongodb');
      const gqlRows = [
        { id: 1, isEnabled: true, table: '507f1f77bcf86cd799439011', tableId: undefined },
      ];
      const tableRows = [
        { _id: '507f1f77bcf86cd799439011', name: 'tasks' },
      ];

      const find = jest.fn(async (params: any) => {
        if (params.table === 'gql_definition') return { data: gqlRows };
        if (params.table === 'table_definition') return { data: tableRows };
        return { data: [] };
      });
      const qb = { find } as any;
      const ee = new EventEmitter2();
      const svc = new GqlDefinitionCacheService(qb, ee);
      await svc.reload(false);

      expect(await svc.isEnabledForTable('tasks')).toBe(true);
    });

    it('skips rows where table resolution returns no name', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const gqlRows = [
        { id: 1, isEnabled: true, tableId: 99, table: null },
      ];
      const tableRows: any[] = [];

      const find = jest.fn(async (params: any) => {
        if (params.table === 'gql_definition') return { data: gqlRows };
        if (params.table === 'table_definition') return { data: tableRows };
        return { data: [] };
      });
      const qb = { find } as any;
      const ee = new EventEmitter2();
      const svc = new GqlDefinitionCacheService(qb, ee);
      await svc.reload(false);

      expect(await svc.isEnabledForTable('tasks')).toBe(false);
      expect(await svc.getAllEnabled()).toEqual([]);
    });
  });

  describe('cache invalidation and reload', () => {
    it('reloading cache replaces old data', async () => {
      DatabaseConfigService.overrideForTesting('mysql');

      let callCount = 0;
      const find = jest.fn(async (params: any) => {
        if (params.table === 'gql_definition') {
          callCount++;
          if (callCount === 1) {
            return { data: [makeRow({ id: 1, tableName: 'tasks', isEnabled: true })] };
          }
          return { data: [makeRow({ id: 1, tableName: 'tasks', isEnabled: false })] };
        }
        return { data: [] };
      });
      const qb = { find } as any;
      const ee = new EventEmitter2();
      const svc = new GqlDefinitionCacheService(qb, ee);

      await svc.reload(false);
      expect(await svc.isEnabledForTable('tasks')).toBe(true);

      await svc.reload(false);
      expect(await svc.isEnabledForTable('tasks')).toBe(false);
    });

    it('deduplicates concurrent reload calls', async () => {
      DatabaseConfigService.overrideForTesting('mysql');

      const find = jest.fn(async (params: any) => {
        if (params.table === 'gql_definition') {
          return { data: [makeRow({ tableName: 'tasks' })] };
        }
        return { data: [] };
      });
      const qb = { find } as any;
      const ee = new EventEmitter2();
      const svc = new GqlDefinitionCacheService(qb, ee);

      await Promise.all([svc.reload(false), svc.reload(false)]);

      expect(find.mock.calls.filter((c: any) => c[0].table === 'gql_definition').length).toBe(1);
    });
  });

  describe('getLogCount()', () => {
    it('reports correct definition count', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const svc = await createCache([
        makeRow({ tableName: 'a' }),
        makeRow({ tableName: 'b' }),
        makeRow({ tableName: 'c' }),
      ]);

      const raw = svc.getRawCache();
      expect(raw.size).toBe(3);
    });
  });

  describe('matrix: isEnabled across all combinations', () => {
    const cases = [
      { isEnabled: true, expected: true },
      { isEnabled: false, expected: false },
      { isEnabled: undefined, expected: true },
      { isEnabled: null, expected: true },
      { isEnabled: 0, expected: true },
      { isEnabled: 1, expected: true },
    ] as const;

    it('handles all isEnabled value types', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      for (const { isEnabled, expected } of cases) {
        const rows = [{ id: 1, table: { name: 'test_table' }, isEnabled, isSystem: false }];
        const find = jest.fn(async (params: any) => {
          if (params.table === 'gql_definition') return { data: rows };
          return { data: [] };
        });
        const qb = { find } as any;
        const ee = new EventEmitter2();
        const svc = new GqlDefinitionCacheService(qb, ee);
        await svc.reload(false);

        const result = await svc.isEnabledForTable('test_table');
        expect(result).toBe(expected);
      }
    });
  });
});
