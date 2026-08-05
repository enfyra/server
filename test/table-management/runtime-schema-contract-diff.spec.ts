import { formatRuntimeSchemaContractDiff } from '../../src/modules/table-management/utils/runtime-schema-contract-diff.util';
import { normalizeRuntimeTableSchema } from '../../src/modules/table-management/utils/runtime-schema-normalization.util';
import { hashCanonical } from '../../src/shared/utils/schema-mutation-contract.util';

const baseColumn = {
  id: 2,
  name: 'promptTokens',
  type: 'bigint',
  isPrimary: false,
  isGenerated: false,
  isNullable: false,
  description: null,
  values: null,
  isPublished: true,
  isUpdatable: true,
  isEncrypted: false,
  options: null,
  metadata: null,
  placeholder: null,
};

const baseTable = {
  name: 'zz_repro',
  description: null,
  alias: null,
  isSingleRecord: false,
  graphqlEnabled: true,
  validateBody: true,
  relations: [],
  uniques: [],
  indexes: [],
};

describe('normalizeRuntimeTableSchema json-string canonicalization', () => {
  it('hashes intent string defaultValue the same as persisted parsed value', () => {
    const intent = normalizeRuntimeTableSchema(
      {
        ...baseTable,
        columns: [{ ...baseColumn, id: undefined, defaultValue: '0' }],
      },
      { backend: 'postgresql', mode: 'intent' },
    );
    const persisted = normalizeRuntimeTableSchema(
      {
        ...baseTable,
        columns: [{ ...baseColumn, defaultValue: 0 }],
      },
      { backend: 'postgresql', mode: 'persisted' },
    );

    expect(intent).not.toBeNull();
    expect(persisted).not.toBeNull();
    expect(hashCanonical(intent!.contract)).toBe(
      hashCanonical(persisted!.contract),
    );
  });

  it('hashes intent string options/metadata the same as persisted parsed values', () => {
    const intent = normalizeRuntimeTableSchema(
      {
        ...baseTable,
        columns: [
          {
            ...baseColumn,
            id: undefined,
            type: 'enum',
            options: '["a","b"]',
            metadata: '{"richText":{"toolbar":"full"}}',
          },
        ],
      },
      { backend: 'postgresql', mode: 'intent' },
    );
    const persisted = normalizeRuntimeTableSchema(
      {
        ...baseTable,
        columns: [
          {
            ...baseColumn,
            type: 'enum',
            options: ['a', 'b'],
            metadata: { richText: { toolbar: 'full' } },
          },
        ],
      },
      { backend: 'postgresql', mode: 'persisted' },
    );

    expect(hashCanonical(intent!.contract)).toBe(
      hashCanonical(persisted!.contract),
    );
  });

  it('keeps plain string defaultValue unchanged', () => {
    const intent = normalizeRuntimeTableSchema(
      {
        ...baseTable,
        columns: [
          { ...baseColumn, id: undefined, type: 'varchar', defaultValue: 'abc' },
        ],
      },
      { backend: 'postgresql', mode: 'intent' },
    );
    expect(intent!.contract.columns[0].defaultValue).toBe('abc');
  });
});

describe('formatRuntimeSchemaContractDiff coverage', () => {
  const makeContract = (defaultValue: unknown) =>
    normalizeRuntimeTableSchema(
      {
        ...baseTable,
        columns: [{ ...baseColumn, defaultValue }],
      },
      { backend: 'postgresql', mode: 'persisted' },
    )!.contract;

  it('reports defaultValue differences instead of no-diff-detected', () => {
    const diff = formatRuntimeSchemaContractDiff(
      makeContract(0),
      makeContract(5),
    );
    expect(diff).toContain('defaultValue');
    expect(diff).not.toBe('no-diff-detected');
  });

  it('reports options and metadata differences', () => {
    const left = normalizeRuntimeTableSchema(
      {
        ...baseTable,
        columns: [
          {
            ...baseColumn,
            type: 'enum',
            options: ['a'],
            metadata: { richText: { toolbar: 'full' } },
          },
        ],
      },
      { backend: 'postgresql', mode: 'persisted' },
    )!.contract;
    const right = normalizeRuntimeTableSchema(
      {
        ...baseTable,
        columns: [
          {
            ...baseColumn,
            type: 'enum',
            options: ['a', 'b'],
            metadata: { richText: { toolbar: 'minimal' } },
          },
        ],
      },
      { backend: 'postgresql', mode: 'persisted' },
    )!.contract;

    const diff = formatRuntimeSchemaContractDiff(left, right);
    expect(diff).toContain('options');
    expect(diff).toContain('metadata');
  });

  it('reports relation description differences', () => {
    const makeRelationContract = (description: string) =>
      normalizeRuntimeTableSchema(
        {
          ...baseTable,
          columns: [],
          relations: [
            {
              propertyName: 'user',
              type: 'many-to-one',
              targetTableName: 'enfyra_user',
              isNullable: false,
              description,
            },
          ],
        },
        { backend: 'postgresql', mode: 'persisted' },
      )!.contract;

    const diff = formatRuntimeSchemaContractDiff(
      makeRelationContract('owner'),
      makeRelationContract(''),
    );
    expect(diff).toContain('description');
    expect(diff).not.toBe('no-diff-detected');
  });

  it('returns no-diff-detected only when contracts are identical', () => {
    const contract = makeContract(0);
    expect(formatRuntimeSchemaContractDiff(contract, contract)).toBe(
      'no-diff-detected',
    );
  });
});
