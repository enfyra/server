import { describe, expect, it } from 'vitest';
import { defaultData } from '../../src/data';
import {
  BootstrapDefinitionService,
  MetadataMigrationService,
} from '../../src/engines/bootstrap';
import { compileMetadataMigrationExecutionPlan } from '../../src/engines/bootstrap/utils/metadata-migration-plan.util';

describe('BootstrapDefinitionService', () => {
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
          defaultData,
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
