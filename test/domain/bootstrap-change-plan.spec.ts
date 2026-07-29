import { buildBootstrapChangePlan } from '../../src/engines/bootstrap/utils/bootstrap-change-plan.util';

describe('buildBootstrapChangePlan', () => {
  it('weights install stages by expected runtime instead of raw change count', () => {
    const plan = buildBootstrapChangePlan(
      {
        mode: 'install',
        database: 'postgresql',
        targetTableCount: 2,
        observedMetadata: { tables: 0, columns: 0, relations: 0 },
        operations: [],
        phases: [],
      },
      {
        snapshot: { users: {}, enfyra_route: {} },
        migration: null,
        defaultData: {
          users: [{ email: 'a' }, { email: 'b' }],
          enfyra_route: [{ path: '/users' }],
        },
        dataMigration: {},
        dataTargetSnapshot: {},
      },
    );

    expect(plan.changes).toHaveLength(12);
    expect(
      plan.changes.filter((change) => change.stage === 'schema'),
    ).toHaveLength(2);
    expect(
      plan.changes.filter((change) => change.stage === 'defaults'),
    ).toHaveLength(1);
    expect(
      plan.changes.filter((change) => change.stage === 'handlers'),
    ).toHaveLength(1);
    const weightFor = (...stages: string[]) =>
      plan.changes
        .filter((change) => stages.includes(change.stage))
        .reduce((total, change) => total + change.weight, 0);
    expect(weightFor('schema')).toBe(40);
    expect(weightFor('metadata')).toBe(15);
    expect(weightFor('healing')).toBe(10);
    expect(weightFor('cache')).toBe(15);
    expect(weightFor('defaults', 'data')).toBe(10);
    expect(weightFor('handlers')).toBe(5);
    expect(weightFor('attestation', 'finalize')).toBe(5);
    expect(weightFor(...plan.changes.map((change) => change.stage))).toBe(100);
    expect(Object.isFrozen(plan.changes)).toBe(true);
  });

  it('uses atomic migration diffs instead of unchanged snapshot tables on upgrade', () => {
    const plan = buildBootstrapChangePlan(
      {
        mode: 'upgrade',
        database: 'mongodb',
        targetTableCount: 20,
        observedMetadata: { tables: 20, columns: 100, relations: 15 },
        operations: [
          {
            id: 'schema:modify-column:users.email',
            label: 'modify column users.email',
            kind: 'modify-column',
            tableName: 'users',
            modification: {
              from: { name: 'email', type: 'varchar' },
              to: { name: 'email', type: 'text' },
            },
          },
          {
            id: 'schema:remove-relation:users.legacy',
            label: 'remove relation users.legacy',
            kind: 'remove-relation',
            tableName: 'users',
            propertyName: 'legacy',
          },
        ],
        phases: [],
      },
      {
        snapshot: Object.fromEntries(
          Array.from({ length: 20 }, (_, index) => [`table_${index}`, {}]),
        ),
        migration: null,
        defaultData: { users: [{ email: 'unchanged' }] },
        dataMigration: {},
        dataTargetSnapshot: {},
      },
    );

    expect(
      plan.changes.filter((change) => change.stage === 'schema'),
    ).toHaveLength(2);
    expect(plan.changes.some((change) => change.stage === 'defaults')).toBe(
      true,
    );
    expect(plan.changes[0].weight).toBe(20);
    expect(plan.changes[1].weight).toBe(20);
  });
});
