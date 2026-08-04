import { buildRuntimeSchemaChangePlan } from '../../src/modules/table-management/utils/runtime-schema-change-plan.util';
import { normalizeRuntimeTableSchema } from '../../src/modules/table-management/utils/runtime-schema-normalization.util';

const table = {
  name: 'settings',
  description: null,
  alias: null,
  isSingleRecord: true,
  graphqlEnabled: true,
  validateBody: true,
  columns: [
    {
      id: 1,
      name: 'id',
      type: 'int',
      isPrimary: true,
      isGenerated: false,
      isNullable: false,
      defaultValue: null,
      description: null,
      values: null,
      isPublished: true,
      isUpdatable: true,
      isEncrypted: false,
      options: null,
      metadata: null,
      placeholder: null,
    },
  ],
  relations: [],
  uniques: [],
  indexes: [],
};

describe('runtime schema table metadata changes', () => {
  it('creates an executable metadata change when isSingleRecord changes', () => {
    const before = normalizeRuntimeTableSchema(table, { backend: 'postgresql' });
    const after = normalizeRuntimeTableSchema(
      { ...table, isSingleRecord: false },
      { backend: 'postgresql' },
    );

    const plan = buildRuntimeSchemaChangePlan({
      operation: 'update',
      tableName: table.name,
      before,
      after,
      owningSideInverseCascadeWarnings: [],
    });

    expect(plan.changes).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ kind: 'alter-table-metadata' }),
      ]),
    );
    expect(plan.diff.schemaChanged).toBe(false);
  });
});
