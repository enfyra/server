import { expect, it } from 'vitest';
import { QueryBuilderService } from '@enfyra/kernel';
import { DynamicReadAuthorizationService } from '../../src/modules/dynamic-api/services/dynamic-read-authorization.service';
import { DynamicRepositoryReadService } from '../../src/modules/dynamic-api/services/dynamic-repository-read.service';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

it.each([false, true])(
  'preserves read authorization through the installed Mongo query builder (conditional=%s)',
  async (conditional) => {
    DatabaseConfigService.overrideForTesting('mongodb');
    const table = {
      name: 'employees',
      columns: [
        { name: '_id', type: 'objectId', isPrimary: true, isPublished: true },
        { name: 'salary', type: 'int', isPublished: true },
        { name: 'ownerId', type: 'string', isPublished: false },
      ],
      relations: [],
    };
    const metadata = {
      tables: new Map([['employees', table]]),
      tablesList: [table],
    };
    const rows = [
      { _id: 'one', salary: 100, ownerId: 'reader' },
      { _id: 'two', salary: 200, ownerId: 'other' },
    ];
    const registry: any = {
      lookupTableByName: () => table,
      requireMetadata: () => metadata,
      getMaxQueryDepth: () => 10,
      getFieldPermissionPoliciesFor: () => [
        {
          rules: [
            {
              tableName: 'employees',
              columnName: 'salary',
              action: 'read',
              effect: 'deny',
              isEnabled: true,
              roleId: null,
              allowedUserIds: [],
              condition: conditional ? { ownerId: { _eq: '@USER.id' } } : null,
            },
          ],
        },
      ],
    };
    const collection = {
      aggregate: (pipeline: any[]) => ({
        toArray: async () =>
          rows.map((row) => {
            let result: Record<string, unknown> = { ...row };
            for (const stage of pipeline) {
              if (!stage.$project) continue;
              const projection = stage.$project;
              const inclusive = Object.entries(projection).some(
                ([key, value]) => key !== '_id' && value === 1,
              );
              if (inclusive)
                result = Object.fromEntries(
                  Object.entries(result).filter(
                    ([key]) =>
                      projection[key] === 1 ||
                      (key === '_id' && projection._id !== 0),
                  ),
                );
              else
                for (const [key, value] of Object.entries(projection))
                  if (value === 0) delete result[key];
            }
            return result;
          }),
      }),
    };
    const builder = new QueryBuilderService({
      databaseConfigService: {
        getDbType: () => 'mongodb',
        isMongoDb: () => true,
      },
      mongoService: {
        getDb: () => ({ collection: () => collection }),
        parseResult: async (rows: any) => rows,
      },
      lazyRef: {
        metadataCacheService: {
          isLoaded: () => true,
          getMetadata: async () => metadata,
        },
      },
    } as any);
    try {
      const reader = new DynamicRepositoryReadService({
        context: { $user: { id: 'reader' }, $query: {} } as any,
        enforceFieldPermission: true,
        queryBuilderService: builder,
        readAuthorizationService: new DynamicReadAuthorizationService({
          runtimeRegistryService: registry,
        }),
        runtimeRegistryService: registry,
        tableName: 'employees',
      });
      expect((await reader.find({ fields: 'salary', meta: [] })).data).toEqual(
        conditional ? [{}, { salary: 200 }] : [{}, {}],
      );
    } finally {
      await builder.flushBatchInserts();
      DatabaseConfigService.overrideForTesting('mysql');
    }
  },
);
