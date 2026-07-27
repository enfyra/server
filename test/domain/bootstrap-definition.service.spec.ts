import { describe, expect, it } from 'vitest';
import { dataMigration, defaultData } from '../../src/data';
import {
  BootstrapDefinitionService,
  MetadataMigrationService,
} from '../../src/engines/bootstrap';
import { compileMetadataMigrationExecutionPlan } from '../../src/engines/bootstrap/utils/metadata-migration-plan.util';

describe('BootstrapDefinitionService', () => {
  it('loads and freezes the current bootstrap target once', () => {
    const service = new BootstrapDefinitionService();
    const definition = service.getDefinition();

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
          migration: {
            tables: [],
            tablesToRename: [{ from: 'legacy', to: 'missing' }],
          },
          defaultData,
          dataMigration,
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
      bootstrapDefinitionService.getMigration(),
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
