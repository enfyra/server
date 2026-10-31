import { EventEmitter2 } from 'eventemitter2';
import { BatchFetchEngine, validateDeepOptions } from '@enfyra/kernel';
import { afterEach, describe, expect, it, vi } from 'vitest';
import snapshot from '../../src/data/snapshot';
import { RouteCacheService } from '../../src/engines/cache/services/route-cache.service';
import { GuardCacheBuilder } from '../../src/engines/cache/services/guard-cache-builder.service';
import { GqlDefinitionCacheService } from '../../src/engines/cache/services/gql-definition-cache.service';
import { DatabaseConfigService } from '../../src/shared/services';
import { loadUserWithRoles } from '../../src/shared/utils/load-user-with-role.util';

function metadata() {
  const tables = new Map<string, any>(
    Object.entries(snapshot).map(([name, table]) => [
      name,
      {
        name,
        columns: table.columns.map((column) => ({
          name: column.name,
          type: 'string',
        })),
        relations: (table.relations ?? []).map((relation) => ({
          ...relation,
          targetTableName: relation.targetTable,
        })),
      },
    ]),
  );
  for (const table of Object.values(snapshot)) {
    for (const relation of table.relations ?? []) {
      if (!relation.inversePropertyName) continue;
      const target = tables.get(relation.targetTable)!;
      if (
        target.relations.some(
          (item: any) => item.propertyName === relation.inversePropertyName,
        )
      )
        continue;
      target.relations.push({
        propertyName: relation.inversePropertyName,
        type:
          relation.type === 'many-to-one'
            ? 'one-to-many'
            : relation.type === 'one-to-many'
              ? 'many-to-one'
              : relation.type,
        targetTableName: table.name,
        isInverse: true,
      });
    }
  }
  return { tables };
}

afterEach(() => DatabaseConfigService.resetForTesting());

describe('reload queries against the installed Kernel contract', () => {
  it('validates each explicit deep graph against current system relation cardinality', async () => {
    DatabaseConfigService.overrideForTesting('postgres');
    const queries: any[] = [];
    const find = vi.fn(async (options: any) => {
      queries.push(options);
      return { data: [] };
    });
    const deps: any = {
      eventEmitter: new EventEmitter2(),
      queryBuilderService: { find, isMongoDb: () => false },
      metadataCacheService: { getMetadata: async () => ({ tablesList: [] }) },
    };
    await new RouteCacheService(deps).reload(false);
    await new GuardCacheBuilder(deps).reload(false);
    await new GqlDefinitionCacheService(deps).reload(false);
    await loadUserWithRoles(
      {
        isMongoDb: () => false,
        findOne: async (options: any) => {
          queries.push(options);
          return null;
        },
      } as any,
      '1',
    );
    const schema = metadata();
    for (const query of queries.filter((query) => query.deep)) {
      expect(() =>
        validateDeepOptions(query.table, query.deep, schema, 0, 7),
      ).not.toThrow();
      const descriptors = Object.entries(query.deep).map(
        ([name, entry]: [string, any]) => {
          const relation = schema.tables
            .get(query.table)
            .relations.find((item: any) => item.propertyName === name);
          return {
            relationName: name,
            type: relation.type,
            targetTable: relation.targetTableName,
            fields: [],
            userLimit: entry.limit,
            nestedDeep: entry.deep,
          };
        },
      );
      const engine = new BatchFetchEngine(
        {
          resolveFields: () => ({ nestedDescs: [], fetchSpec: {} }),
        } as any,
        async (name) => schema.tables.get(name),
      );
      await expect(
        engine.execute([], descriptors, 7, 0, query.table),
      ).resolves.toBeUndefined();
    }
    expect(queries.filter((query) => query.deep)).toHaveLength(7);
  });
});
