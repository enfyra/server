import {
  assertSchemaMutationPlan,
  buildSchemaMutationExecutionPhases,
} from '../../src/shared/utils/schema-mutation-plan.util';

const changes = [
  { id: 'change:table', kind: 'table', label: 'update table' },
  { id: 'change:relation', kind: 'relation', label: 'update relation' },
];

describe('schema mutation plan', () => {
  it('derives unbounded phases while preserving parallel nodes', () => {
    const nodes = [
      {
        id: 'attest',
        changeId: 'change:table',
        dependsOn: [],
        completesChange: false,
        command: { kind: 'attest' },
      },
      {
        id: 'physical',
        changeId: 'change:table',
        dependsOn: ['attest'],
        completesChange: false,
        command: { kind: 'physical' },
      },
      {
        id: 'metadata',
        changeId: 'change:table',
        dependsOn: ['physical'],
        completesChange: true,
        command: { kind: 'metadata' },
      },
      {
        id: 'relation',
        changeId: 'change:relation',
        dependsOn: ['physical'],
        completesChange: true,
        command: { kind: 'relation' },
      },
    ];

    assertSchemaMutationPlan(changes, nodes);
    const phases = buildSchemaMutationExecutionPhases(nodes);

    expect(phases.map((phase) => phase.index)).toEqual([0, 1, 2]);
    expect(phases[2].nodes.map((node) => node.id)).toEqual([
      'metadata',
      'relation',
    ]);
    expect(Object.isFrozen(phases[2].nodes[0].command)).toBe(true);
  });

  it.each([
    {
      name: 'duplicate changes',
      changes: [changes[0], changes[0]],
      nodes: [],
      message: /duplicate change ids/,
    },
    {
      name: 'missing change',
      changes,
      nodes: [
        {
          id: 'node',
          changeId: 'missing',
          dependsOn: [],
          completesChange: true,
          command: {},
        },
      ],
      message: /references missing change missing/,
    },
    {
      name: 'missing completion',
      changes: [changes[0]],
      nodes: [
        {
          id: 'node',
          changeId: changes[0].id,
          dependsOn: [],
          completesChange: false,
          command: {},
        },
      ],
      message: /complete each change exactly once/,
    },
    {
      name: 'duplicate completion',
      changes: [changes[0]],
      nodes: ['left', 'right'].map((id) => ({
        id,
        changeId: changes[0].id,
        dependsOn: [],
        completesChange: true,
        command: {},
      })),
      message: /complete each change exactly once/,
    },
  ])('rejects $name', ({ changes: inputChanges, nodes, message }) => {
    expect(() => assertSchemaMutationPlan(inputChanges, nodes)).toThrow(
      message,
    );
  });

  it('rejects missing dependencies and cycles', () => {
    expect(() =>
      buildSchemaMutationExecutionPhases([
        {
          id: 'broken',
          changeId: changes[0].id,
          dependsOn: ['missing'],
          completesChange: true,
          command: {},
        },
      ]),
    ).toThrow(/depends on missing node missing/);

    expect(() =>
      buildSchemaMutationExecutionPhases([
        {
          id: 'left',
          changeId: changes[0].id,
          dependsOn: ['right'],
          completesChange: false,
          command: {},
        },
        {
          id: 'right',
          changeId: changes[0].id,
          dependsOn: ['left'],
          completesChange: true,
          command: {},
        },
      ]),
    ).toThrow(/dependency cycle/);
  });
});
