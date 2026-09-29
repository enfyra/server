import { describe, expect, it } from 'vitest';
import { bootstrapSourceArtifacts } from '../../src/data';
import {
  BootstrapDefinitionService,
  MetadataMigrationService,
} from '../../src/engines/bootstrap';
import { compileMetadataMigrationExecutionPlan } from '../../src/engines/bootstrap/utils/metadata-migration-plan.util';

describe('BootstrapDefinitionService', () => {
  it('seeds callable REST routes for method settings and multipart fields', () => {
    const routes = bootstrapSourceArtifacts.defaultData.enfyra_route as any[];
    const methodConfig = routes.find(route => route.path === '/enfyra_route_method_config');
    const fileFields = routes.find(route => route.path === '/enfyra_route_method_config_file_field');
    expect(methodConfig).toMatchObject({ mainTable: 'enfyra_route_method_config', availableMethods: ['GET', 'PATCH'] });
    expect(fileFields).toMatchObject({ mainTable: 'enfyra_route_method_config_file_field', availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'] });
    const migration = new BootstrapDefinitionService().resolveForVersion('2.2.19-patch-1').dataMigration.enfyra_route;
    expect(migration.some((record: any) => record._unique?.path?._eq === methodConfig.path)).toBe(true);
    expect(migration.some((record: any) => record._unique?.path?._eq === fileFields.path)).toBe(true);
  });

  it('rejects unsupported deletion declarations while loading artifacts', () => {
    expect(
      () =>
        new BootstrapDefinitionService(undefined, {
          snapshot: {},
          migrations: [],
          defaultData: {},
          dataCorrections: {},
          dataMigrations: [
            {
              fromVersion: '2.2.19-patch-1',
              toVersion: '2.3.0',
              data: {
                _deletedRecords: [
                  {
                    table: 'enfyra_route',
                    filter: { path: { _in: ['/logs'] } },
                  },
                ],
              },
            },
          ],
        }),
    ).toThrow(/exact scalar or _eq filter/);
  });
  it('materializes and freezes a Mongo-native bootstrap target without mutating sources', () => {
    const service = new BootstrapDefinitionService({
      databaseConfigService: { getDbType: () => 'mongodb' } as any,
    });
    const definition = service.resolveForVersion('2.2.19-patch-1');
    const packageTable = definition.snapshot.enfyra_package;
    const typeColumn = definition.snapshot.enfyra_column.columns.find(
      (column: any) => column.name === 'type',
    );

    expect(
      packageTable.columns.find((column: any) => column.name === '_id'),
    ).toEqual(expect.objectContaining({ name: '_id', type: 'objectId' }));
    const persistedTypeTarget = definition.dataMigration.enfyra_column.find(
      (record: any) =>
        record._unique?._and?.some(
          (condition: any) => condition?.table?.name?._eq === 'enfyra_column',
        ) &&
        record._unique?._and?.some(
          (condition: any) => condition?.name?._eq === 'type',
        ),
    );

    expect(typeColumn.options).toContain('object');
    expect(typeColumn.options).not.toContain('simple-json');
    expect(persistedTypeTarget.options).toContain('object');
    expect(persistedTypeTarget.options).not.toContain('simple-json');
    expect(Object.isFrozen(definition)).toBe(true);
    expect(Object.isFrozen(definition.snapshot)).toBe(true);
  });

  it('merges both declared schema and data steps from 2.2.19-patch-1', () => {
    const service = new BootstrapDefinitionService();
    const definition = service.resolveForVersion('2.2.19-patch-1');
    const handler = definition.migration?.tables?.find(
      (table) => table._unique.name._eq === 'enfyra_route_handler',
    );
    const setting = definition.migration?.tables?.find(
      (table) => table._unique.name._eq === 'enfyra_setting',
    );
    expect(handler?.columnsToModify?.map((column) => column.from.name)).toContain('sourceCode');
    expect(handler?.tableToModify?.to.uniques).toEqual([
      ['route', 'method'],
      ['routeMethodConfig'],
    ]);
    expect(setting?.columnsToRemove).toContain('uniquesIndexesRepaired');
    expect(definition.dataMigration.enfyra_column.some(
      (record: any) => record._unique?._and?.some(
        (condition: any) => condition?.name?._eq === 'type',
      ) && record.type === 'enum' && Array.isArray(record.options),
    )).toBe(true);
  });

  it('loads and freezes the current SQL bootstrap target once', () => {
    const service = new BootstrapDefinitionService();
    const definition = service.resolveForVersion('2.2.19-patch-1');

    expect(definition.snapshot.enfyra_setting.name).toBe('enfyra_setting');
    expect(definition.dataTargetSnapshot.enfyra_file.validateBody).toBe(false);
    expect(Object.isFrozen(definition)).toBe(true);
    expect(Object.isFrozen(definition.snapshot)).toBe(true);
    expect(Object.isFrozen(definition.snapshot.enfyra_setting.columns)).toBe(
      true,
    );
  });

  it('fails before runtime mutation when a migration target is invalid', () => {
    expect(
      () =>
        new BootstrapDefinitionService(undefined, {
          snapshot: {
            current: { name: 'current', columns: [], relations: [] },
          },
          migrations: [
            {
              fromVersion: '2.2.19-patch-1',
              toVersion: '2.3.0',
              schema: {
                tables: [],
                tablesToRename: [{ from: 'legacy', to: 'missing' }],
              },
            },
          ],
          defaultData: bootstrapSourceArtifacts.defaultData,
          dataCorrections: {},
          dataMigrations: [],
        }),
    ).toThrow(/target missing does not exist in snapshot\.ts/);
  });

  it('builds an immutable install plan before migration execution', async () => {
    const bootstrapDefinitionService = new BootstrapDefinitionService();
    const service = new MetadataMigrationService({
      bootstrapDefinitionService,
      queryBuilderService: {
        isMongoDb: () => false,
        getKnex: () => ({
          client: { config: { client: 'pg' } },
          schema: { hasTable: async () => false },
        }),
      } as any,
      systemCoreTableResolver: {
        getNames: async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        }),
      } as any,
    });

    const plan = await service.prepareMigrationExecutionPlan();

    expect(plan.mode).toBe('install');
    expect(plan.database).toBe('postgresql');
    expect(plan.targetTableCount).toBeGreaterThan(0);
    expect(plan.operations).toEqual([]);
    expect(plan.phases).toEqual([]);
    expect(Object.isFrozen(plan)).toBe(true);
    expect(Object.isFrozen(plan.operations)).toBe(true);
    expect(Object.isFrozen(plan.phases)).toBe(true);
    expect(plan.phases.every((phase, index) => phase.index === index)).toBe(
      true,
    );
    expect(service.getExecutionPlan()).toBe(plan);
  });

  it('compiles same-type code declarations as SQL physical-only migrations', () => {
    const migration = {
      tables: [
        {
          _unique: { name: { _eq: 'enfyra_extension' } },
          columnsToModify: [
            {
              from: { name: 'sourceCode', type: 'code' },
              to: { name: 'sourceCode', type: 'code' },
            },
          ],
        },
      ],
    };
    const context = {
      mode: 'upgrade' as const,
      targetTableCount: 1,
      observedMetadata: { tables: 1, columns: 1, relations: 0 },
    };

    const sqlPlan = compileMetadataMigrationExecutionPlan(migration, {
      ...context,
      database: 'mysql',
    });
    const sqlNodes = sqlPlan.phases.flatMap((phase) => phase.nodes);

    expect(sqlPlan.operations).toHaveLength(1);
    expect(sqlNodes).toEqual([
      expect.objectContaining({
        completesChange: true,
        command: expect.objectContaining({ kind: 'apply-physical-change' }),
      }),
    ]);

    const mongoPlan = compileMetadataMigrationExecutionPlan(migration, {
      ...context,
      database: 'mongodb',
    });

    expect(mongoPlan.operations).toEqual([]);
    expect(mongoPlan.phases).toEqual([]);
  });

  it('projects every applicable schema step to the active SQL contract', () => {
    const service = new BootstrapDefinitionService();
    service.resolveForVersion('2.2.19-patch-1');
    const steps = service.getApplicableSchemaSteps();
    const typeChange = steps[0].schema.tables?.find(
      (table) => table._unique.name._eq === 'enfyra_column',
    )?.columnsToModify?.find((column) => column.from.name === 'type');

    expect(steps.map((step) => step.toVersion)).toEqual(['2.3.0', '2.3.1']);
    expect(typeChange?.from.type).toBe('varchar');
    expect(typeChange?.to.type).toBe('enum');
    expect(typeChange?.to.options).toContain('simple-json');
    expect(typeChange?.to.sqlType).toBeUndefined();
  });

  it('executes each version entirely before the next version starts', async () => {
    const service = new MetadataMigrationService({
      bootstrapDefinitionService: new BootstrapDefinitionService(),
      queryBuilderService: {
        isMongoDb: () => false,
        getKnex: () => ({ client: { config: { client: 'pg' } } }),
      } as any,
      systemCoreTableResolver: { getNames: async () => ({}) } as any,
    });
    const steps = [
      {
        fromVersion: '2.2.19-patch-1',
        toVersion: '2.3.0',
        schema: {
          tables: [{
            _unique: { name: { _eq: 'enfyra_column' } },
            columnsToModify: [{
              from: { name: 'type', type: 'varchar' },
              to: { name: 'type', type: 'enum', options: ['varchar', 'enum'] },
            }],
          }],
        },
      },
      {
        fromVersion: '2.3.0',
        toVersion: '2.3.1',
        schema: { coreTablesToRename: [{ from: 'old_core', to: 'new_core' }] },
      },
    ];
    const plan = compileMetadataMigrationExecutionPlan(null, {
      mode: 'upgrade',
      database: 'postgresql',
      targetTableCount: 1,
      observedMetadata: { tables: 1, columns: 1, relations: 0 },
      steps,
    });
    (service as any).executionPlan = plan;
    const executed: string[] = [];
    (service as any).executePlanCommand = async (command: any) => {
      executed.push(command.operation.id);
    };

    await service.executeCoreMigrationPlan();
    await service.executeRemainingMigrationPlan();

    expect(executed.filter((id) => id.startsWith('2.3.0:'))).toHaveLength(2);
    expect(executed.filter((id) => id.startsWith('2.3.1:'))).toHaveLength(2);
    expect(executed.findIndex((id) => id.startsWith('2.3.1:'))).toBe(2);
  });

  it('executes compiled nodes by dynamic phase and completes each logical change once', async () => {
    const bootstrapDefinitionService = new BootstrapDefinitionService();
    const service = new MetadataMigrationService({
      bootstrapDefinitionService,
      queryBuilderService: {
        isMongoDb: () => false,
        getKnex: () => ({
          client: { config: { client: 'pg' } },
          schema: { hasTable: async () => false },
        }),
      } as any,
      systemCoreTableResolver: {
        getNames: async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        }),
      } as any,
    });
    const plan = compileMetadataMigrationExecutionPlan(
      bootstrapDefinitionService.resolveForVersion('2.2.19-patch-1').migration,
      {
        mode: 'upgrade',
        database: 'postgresql',
        targetTableCount: 1,
        observedMetadata: { tables: 1, columns: 1, relations: 1 },
      },
    );
    (service as any).executionPlan = plan;
    const executeCommand = jest.fn().mockImplementation(async () => undefined);
    (service as any).executePlanCommand = executeCommand;
    const completed: string[] = [];

    await service.executeCoreMigrationPlan((operation) => {
      completed.push(operation.id);
    });
    await service.executeRemainingMigrationPlan((operation) => {
      completed.push(operation.id);
    });
    await service.executeRemainingMigrationPlan((operation) => {
      completed.push(operation.id);
    });

    expect(executeCommand.mock.calls.map(([command]) => command)).toEqual(
      plan.phases.flatMap((phase) => phase.nodes.map((node) => node.command)),
    );
    expect(new Set(completed)).toEqual(
      new Set(plan.operations.map((operation) => operation.id)),
    );
    expect(new Set(completed).size).toBe(plan.operations.length);
  });
});
