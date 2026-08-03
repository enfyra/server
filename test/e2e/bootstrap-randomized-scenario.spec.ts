import { describe, expect, it } from 'vitest';
import { bootstrapSourceArtifacts } from '../../src/data';
import { BootstrapDefinitionService } from '../../src/engines/bootstrap/services/bootstrap-definition.service';
import { createRandomizedBootstrapScenario } from './bootstrap-randomized-scenario';
import type { BootstrapRandomizedOperationKind } from './types/bootstrap-randomized-matrix.types';

const REQUIRED_OPERATIONS = new Set<BootstrapRandomizedOperationKind>([
  'table-add',
  'table-drop',
  'table-rename',
  'column-add',
  'column-remove',
  'column-rename',
  'column-modify',
  'relation-add',
  'relation-remove',
  'relation-rename',
  'index-modify',
  'unique-modify',
]);

describe('randomized bootstrap scenario', () => {
  it('is deterministic for the same seed and varies across seeds', () => {
    expect(createRandomizedBootstrapScenario(101)).toEqual(
      createRandomizedBootstrapScenario(101),
    );
    expect(createRandomizedBootstrapScenario(101)).not.toEqual(
      createRandomizedBootstrapScenario(102),
    );
  });

  it('builds bootstrap definitions that pass production validation', () => {
    for (const seed of [101, 211, 307, 401, 503]) {
      const scenario = createRandomizedBootstrapScenario(seed);
      expect(
        () => new BootstrapDefinitionService(undefined, scenario.source),
      ).not.toThrow();
      expect(
        () => new BootstrapDefinitionService(undefined, scenario.target),
      ).not.toThrow();
    }
  });

  it('preserves production artifacts outside the isolated prefix', () => {
    const scenario = createRandomizedBootstrapScenario(607);
    for (const [tableName, definition] of Object.entries(
      bootstrapSourceArtifacts.snapshot,
    )) {
      expect(tableName.startsWith(scenario.prefix)).toBe(false);
      expect(scenario.source.snapshot[tableName]).toEqual(definition);
      expect(scenario.target.snapshot[tableName]).toEqual(definition);
    }
    expect(scenario.source.defaultData).toEqual(
      bootstrapSourceArtifacts.defaultData,
    );
    expect(scenario.target.defaultData).toEqual(
      bootstrapSourceArtifacts.defaultData,
    );
    expect(scenario.source.dataMigration).toEqual(
      bootstrapSourceArtifacts.dataMigration,
    );
    expect(scenario.target.dataMigration).toEqual(
      bootstrapSourceArtifacts.dataMigration,
    );
  });

  it('uses safe system prefixes and covers every required operation kind', () => {
    const scenario = createRandomizedBootstrapScenario(701);
    expect(scenario.prefix).toMatch(/^enfyra_matrix_[a-z0-9_]+$/);
    const syntheticTables = [
      ...Object.keys(scenario.source.snapshot),
      ...Object.keys(scenario.target.snapshot),
    ].filter((tableName) => tableName.startsWith(scenario.prefix));
    expect(syntheticTables.length).toBeGreaterThan(0);
    expect(
      syntheticTables.every((tableName) => tableName.startsWith('enfyra_')),
    ).toBe(true);
    const actualKinds = new Set(
      scenario.operations.map((operation) => operation.kind),
    );
    for (const kind of REQUIRED_OPERATIONS) {
      expect(actualKinds.has(kind)).toBe(true);
    }
  });

  it('rejects non-reproducible seeds', () => {
    expect(() => createRandomizedBootstrapScenario(Number.NaN)).toThrow(
      'safe integer',
    );
  });
});
