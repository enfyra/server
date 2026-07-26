import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  BootstrapDefinitionService,
  MetadataMigrationService,
} from '../../src/engines/bootstrap';

describe('BootstrapDefinitionService', () => {
  it('loads and freezes the current bootstrap target once', () => {
    const service = new BootstrapDefinitionService({
      bootstrapDataRoot: process.cwd(),
    });
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
    const root = fs.mkdtempSync(
      path.join(os.tmpdir(), 'enfyra-bootstrap-definition-'),
    );
    fs.mkdirSync(path.join(root, 'data'));
    fs.writeFileSync(
      path.join(root, 'data', 'snapshot.json'),
      JSON.stringify({
        current: { name: 'current', columns: [], relations: [] },
      }),
    );
    fs.writeFileSync(
      path.join(root, 'data', 'snapshot-migration.json'),
      JSON.stringify({
        tables: [],
        tablesToRename: [{ from: 'legacy', to: 'missing' }],
      }),
    );
    fs.writeFileSync(
      path.join(root, 'data', 'default-data.json'),
      JSON.stringify({}),
    );
    fs.writeFileSync(
      path.join(root, 'data', 'data-migration.json'),
      JSON.stringify({}),
    );

    expect(
      () =>
        new BootstrapDefinitionService({
          bootstrapDataRoot: root,
        }),
    ).toThrow(/target missing does not exist in snapshot\.json/);
  });

  it('builds an immutable install plan before migration execution', async () => {
    const bootstrapDefinitionService = new BootstrapDefinitionService({
      bootstrapDataRoot: process.cwd(),
    });
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
