import { describe, expect, it } from 'vitest';
import { dataMigration, defaultData } from '../../src/data';
import {
  BootstrapDefinitionService,
  MetadataMigrationService,
} from '../../src/engines/bootstrap';

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
    expect(plan.database).toBe('sql');
    expect(plan.targetTableCount).toBeGreaterThan(0);
    expect(plan.operations.tableRenames).toContain(
      'route_definition->enfyra_route',
    );
    expect(Object.isFrozen(plan)).toBe(true);
    expect(Object.isFrozen(plan.operations)).toBe(true);
    expect(service.getExecutionPlan()).toBe(plan);
  });
});
