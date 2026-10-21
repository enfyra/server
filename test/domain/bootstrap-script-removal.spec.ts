import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import { bootstrapSourceArtifacts } from '../../src/data';
import { BootstrapDefinitionService } from '../../src/engines/bootstrap';
import { DataMigrationService } from '../../src/engines/bootstrap/services/data-migration.service';
import { DatabaseConfigService } from '../../src/shared/services';
import { compileMetadataMigrationExecutionPlan } from '../../src/engines/bootstrap/utils/metadata-migration-plan.util';
import { deleteMongoMetadataRecords } from '../../src/engines/bootstrap/utils/delete-mongo-metadata-records.util';

const retiredTable = 'enfyra_bootstrap_script';

function mongoFixture() {
  const rows: Record<string, Record<string, any>[]> = {
    enfyra_route: [
      { _id: 'retired-route', path: `/${retiredTable}` },
      { _id: 'kept-route', path: '/orders' },
    ],
    enfyra_route_handler: [
      { _id: 'old-handler', route: 'retired-route' },
      { _id: 'kept-handler', route: 'kept-route' },
    ],
    enfyra_route_method_config: [{ _id: 'old-config', route: 'retired-route' }],
    enfyra_route_method_config_file_field: [
      { _id: 'old-field', routeMethodConfig: 'old-config' },
    ],
    enfyra_pre_hook: [{ _id: 'old-hook', route: 'retired-route' }],
    enfyra_route_permission: [
      { _id: 'old-permission', route: 'retired-route' },
    ],
    enfyra_menu: [
      { _id: 'old-menu', path: '/settings/bootstrap' },
      { _id: 'kept-menu', path: '/settings' },
    ],
    enfyra_menu_permission: [{ _id: 'old-menu-permission', menu: 'old-menu' }],
  };
  const matches = (row: Record<string, any>, filter: Record<string, any>) =>
    Object.entries(filter).every(([key, value]) =>
      value && typeof value === 'object' && '$in' in value
        ? value.$in.includes(row[key])
        : row[key] === value,
    );
  const db = {
    listCollections: ({ name }: { name: string }) => ({
      toArray: async () => (name in rows ? [{ name }] : []),
    }),
    collection: (name: string) => ({
      find: (filter: Record<string, any>) => ({
        toArray: async () =>
          (rows[name] ?? []).filter((row) => matches(row, filter)),
      }),
      findOne: async (filter: Record<string, any>) =>
        (rows[name] ?? []).find((row) => matches(row, filter)) ?? null,
      deleteMany: async (filter: Record<string, any>) => {
        const before = rows[name]?.length ?? 0;
        rows[name] = (rows[name] ?? []).filter((row) => !matches(row, filter));
        return { deletedCount: before - rows[name].length };
      },
      updateMany: vi.fn(async () => ({ modifiedCount: 0 })),
      countDocuments: async (filter: Record<string, any>) =>
        (rows[name] ?? []).filter((row) => matches(row, filter)).length,
    }),
  };
  return { rows, db };
}

describe('custom startup script removal', () => {
  it('has no script table, route, menu, or programmable startup registration on fresh install', () => {
    expect(bootstrapSourceArtifacts.snapshot).not.toHaveProperty(retiredTable);
    expect(JSON.stringify(bootstrapSourceArtifacts.defaultData)).not.toContain(
      retiredTable,
    );
    expect(
      JSON.stringify(bootstrapSourceArtifacts.dataCorrections),
    ).not.toContain(retiredTable);
    for (const path of [
      'src/init.ts',
      'src/wiring/init/phases.ts',
      'src/wiring/registers/bootstrap.ts',
      'src/wiring/cradle.ts',
      'src/engines/cache/services/cache-orchestrator.service.ts',
    ]) {
      expect(readFileSync(path, 'utf8')).not.toMatch(
        /BootstrapScript|bootstrapScript/,
      );
    }
  });

  it.each(['postgresql', 'mysql', 'mongodb'] as const)(
    'declares a reachable targeted drop for %s from 2.3.2',
    (database) => {
      const service = new BootstrapDefinitionService({
        databaseConfigService: { getDbType: () => database } as any,
      });
      const definition = service.resolveForVersion('2.3.2');
      expect(definition.migration?.tablesToDrop).toEqual([retiredTable]);
      expect(
        service.getApplicableSchemaSteps().map((step) => step.toVersion),
      ).toEqual(['2.3.3', '2.3.4']);
      expect(definition.dataMigration._deletedRecords).toEqual([
        {
          table: 'enfyra_route',
          filter: { path: { _eq: `/${retiredTable}` } },
        },
        {
          table: 'enfyra_menu',
          filter: { path: { _eq: '/settings/bootstrap' } },
        },
      ]);
      const plan = compileMetadataMigrationExecutionPlan(definition.migration, {
        mode: 'upgrade',
        database,
        targetTableCount: Object.keys(definition.snapshot).length,
        observedMetadata: { tables: 1, columns: 1, relations: 0 },
        steps: [...service.getApplicableSchemaSteps()],
      });
      expect(plan.operations).toContainEqual(
        expect.objectContaining({
          kind: 'drop-table',
          tableName: retiredTable,
        }),
      );
      expect(JSON.parse(readFileSync('package.json', 'utf8')).version).toBe(
        '2.3.4',
      );
    },
  );

  it('cleans persisted Mongo junctions and is safe to retry', async () => {
    const { rows, db } = mongoFixture();
    rows.enfyra_table = [{ _id: 'route-table', name: 'enfyra_route' }];
    rows.enfyra_relation = [
      {
        sourceTable: 'route-table',
        propertyName: 'availableMethods',
        junctionTableName: 'legacy_route_methods',
        junctionSourceColumn: 'routeKey',
        junctionTargetColumn: 'methodKey',
      },
    ];
    rows.legacy_route_methods = [
      { routeKey: 'retired-route', methodKey: 'get' },
      { routeKey: 'kept-route', methodKey: 'get' },
    ];
    await deleteMongoMetadataRecords(db as any, 'enfyra_route', {
      path: `/${retiredTable}`,
    });
    expect(rows.legacy_route_methods).toEqual([
      { routeKey: 'kept-route', methodKey: 'get' },
    ]);
    expect(
      await deleteMongoMetadataRecords(db as any, 'enfyra_route', {
        path: `/${retiredTable}`,
      }),
    ).toBe(0);
  });

  it('refuses a restricted default page before deleting menu permissions', async () => {
    const { rows, db } = mongoFixture();
    rows.enfyra_setting = [{ _id: 'setting', defaultPage: 'old-menu' }];
    await expect(
      deleteMongoMetadataRecords(db as any, 'enfyra_menu', {
        path: '/settings/bootstrap',
      }),
    ).rejects.toThrow('defaultPage');
    expect(rows.enfyra_menu_permission).toHaveLength(1);
    expect(rows.enfyra_menu).toHaveLength(2);
  });

  it('deletes exact route/menu dependencies on Mongo without deleting other routes', async () => {
    DatabaseConfigService.overrideForTesting('mongodb');
    try {
      const { rows, db } = mongoFixture();
      const service = new DataMigrationService({
        queryBuilderService: {
          getMongoDb: () => db,
          isMongoDb: () => true,
        } as any,
      });
      await (service as any).deleteRecords([
        {
          table: 'enfyra_route',
          filter: { path: { _eq: `/${retiredTable}` } },
        },
        {
          table: 'enfyra_menu',
          filter: { path: { _eq: '/settings/bootstrap' } },
        },
      ]);
      expect(rows.enfyra_route).toEqual([
        { _id: 'kept-route', path: '/orders' },
      ]);
      expect(rows.enfyra_route_handler).toEqual([
        { _id: 'kept-handler', route: 'kept-route' },
      ]);
      for (const table of [
        'enfyra_route_method_config',
        'enfyra_route_method_config_file_field',
        'enfyra_pre_hook',
        'enfyra_route_permission',
        'enfyra_menu_permission',
      ])
        expect(rows[table]).toEqual([]);
    } finally {
      DatabaseConfigService.overrideForTesting('mysql');
    }
  });
});
