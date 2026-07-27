import { buildBootstrapExecutionPhases } from '../../src/engines/bootstrap/utils/bootstrap-execution-phases.util';
import type { BootstrapSchemaOperation } from '../../src/engines/bootstrap/types';

const operation: BootstrapSchemaOperation = {
  id: 'schema:drop-table:legacy',
  label: 'drop table legacy',
  kind: 'drop-table',
  tableName: 'legacy',
};

describe('buildBootstrapExecutionPhases', () => {
  it('derives an unbounded phase number from dependency depth', () => {
    const phases = buildBootstrapExecutionPhases([
      {
        id: 'inspect',
        changeId: operation.id,
        dependsOn: [],
        checkpoint: 'core',
        completesChange: false,
        command: {
          backend: 'postgresql',
          kind: 'apply-physical-change',
          operation,
        },
      },
      {
        id: 'physical',
        changeId: operation.id,
        dependsOn: ['inspect'],
        checkpoint: 'remaining',
        completesChange: false,
        command: {
          backend: 'postgresql',
          kind: 'apply-physical-change',
          operation,
        },
      },
      {
        id: 'metadata',
        changeId: operation.id,
        dependsOn: ['physical'],
        checkpoint: 'remaining',
        completesChange: true,
        command: {
          backend: 'postgresql',
          kind: 'apply-metadata-change',
          operation,
        },
      },
      {
        id: 'independent',
        changeId: operation.id,
        dependsOn: [],
        checkpoint: 'remaining',
        completesChange: false,
        command: {
          backend: 'postgresql',
          kind: 'apply-physical-change',
          operation,
        },
      },
    ]);

    expect(phases.map((phase) => phase.index)).toEqual([0, 1, 2]);
    expect(phases[0].nodes.map((node) => node.id)).toEqual([
      'inspect',
      'independent',
    ]);
    expect(phases[2].nodes[0]).toEqual(
      expect.objectContaining({ id: 'metadata', phase: 2 }),
    );
    expect(Object.isFrozen(phases)).toBe(true);
    expect(Object.isFrozen(phases[0].nodes)).toBe(true);
  });

  it('rejects missing dependencies and cycles before execution', () => {
    expect(() =>
      buildBootstrapExecutionPhases([
        {
          id: 'broken',
          changeId: operation.id,
          dependsOn: ['missing'],
          checkpoint: 'remaining',
          completesChange: true,
          command: {
            backend: 'postgresql',
            kind: 'apply-metadata-change',
            operation,
          },
        },
      ]),
    ).toThrow(/depends on missing node missing/);

    expect(() =>
      buildBootstrapExecutionPhases([
        {
          id: 'left',
          changeId: operation.id,
          dependsOn: ['right'],
          checkpoint: 'remaining',
          completesChange: false,
          command: {
            backend: 'postgresql',
            kind: 'apply-physical-change',
            operation,
          },
        },
        {
          id: 'right',
          changeId: operation.id,
          dependsOn: ['left'],
          checkpoint: 'remaining',
          completesChange: true,
          command: {
            backend: 'postgresql',
            kind: 'apply-metadata-change',
            operation,
          },
        },
      ]),
    ).toThrow(/dependency cycle/);
  });
});
