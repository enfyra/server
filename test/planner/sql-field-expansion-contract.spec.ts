import { describe, expect, it } from 'vitest';
import {
  buildRelationSortSubquery,
  expandFieldsToJoinsAndSelect,
  expandFieldsToSelect,
  parseSortInput,
} from '@enfyra/kernel';

const metadata = {
  name: 'users',
  columns: [
    { name: 'id', type: 'uuid', isPrimary: true },
    { name: 'name', type: 'varchar' },
    { name: 'teamId', type: 'uuid' },
  ],
  relations: [
    {
      propertyName: 'team',
      type: 'many-to-one' as const,
      targetTableName: 'teams',
    },
  ],
};

const metadataGetter = async (tableName: string) =>
  tableName === 'users' ? metadata : null;

describe('SQL field expansion contracts', () => {
  it.each([
    ['id.name', 'children.name'],
    ['children.name', 'id.name'],
  ])('rejects owner alias and injected parent identity collisions independent of order %#', async (...fields) => {
    const source = { name: 'items', columns: [{ name: 'id', isPrimary: true }, { name: 'parent_id' }], relations: [
      { propertyName: 'id', type: 'many-to-one', targetTableName: 'targets', foreignKeyColumn: 'parent_id' },
      { propertyName: 'children', type: 'one-to-many', targetTableName: 'targets', foreignKeyColumn: 'owner_id' },
    ] };
    const target = { name: 'targets', columns: [{ name: 'id', isPrimary: true }, { name: 'name' }, { name: 'owner_id' }], relations: [] };
    await expect(expandFieldsToJoinsAndSelect(
      'items', fields, async (table) => table === 'items' ? source as any : target, 'postgres',
    )).rejects.toThrow(/conflict/i);
  });

  it.each([['id', 'id.name'], ['id.name']])('rejects inverse relation output colliding with the parent primary key %#', async (...fields) => {
    const source = { name: 'items', columns: [{ name: 'id', isPrimary: true }], relations: [
      { propertyName: 'id', type: 'one-to-many', targetTableName: 'targets', foreignKeyColumn: 'owner_id' },
    ] };
    const target = { name: 'targets', columns: [{ name: 'id', isPrimary: true }, { name: 'name' }, { name: 'owner_id' }], relations: [] };
    await expect(expandFieldsToJoinsAndSelect('items', fields, async (table) => table === 'items' ? source as any : target, 'postgres')).rejects.toThrow(/conflict/i);
  });

  it('drops empty sort tokens instead of creating empty identifiers', () => {
    expect(parseSortInput('name,, -, createdAt')).toEqual([
      { field: 'name', direction: 'ASC' },
      { field: 'createdAt', direction: 'ASC' },
    ]);
    expect(parseSortInput(['', '-', 'name'])).toEqual([
      { field: 'name', direction: 'ASC' },
    ]);
  });

  it('strips unknown dotted root fields', async () => {
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['unknown.path'],
        metadataGetter,
        'postgres',
      ),
    ).resolves.toMatchObject({ select: [], batchFetchDescriptors: [] });
  });

  it.each(['.id', 'team.', 'team..name'])(
    'rejects malformed field path %s',
    async (field) => {
      await expect(
        expandFieldsToJoinsAndSelect(
          'users',
          [field],
          metadataGetter,
          'postgres',
        ),
      ).rejects.toThrow(/malformed sql field path/i);
    },
  );

  it('ignores inherited deep relation options', async () => {
    const deepOptions = Object.create({
      team: { fields: ['missing'], limit: 0 },
    });
    const result = await expandFieldsToJoinsAndSelect(
      'users',
      ['team.id'],
      metadataGetter,
      'postgres',
      undefined,
      deepOptions,
    );
    expect(result.batchFetchDescriptors[0]?.fields).toEqual(['id']);
    expect(result.batchFetchDescriptors[0]?.userLimit).toBeUndefined();
  });

  it('ignores inherited properties on own deep relation entries', async () => {
    const entry = Object.create({ fields: ['missing'], limit: 0 });
    const result = await expandFieldsToJoinsAndSelect(
      'users',
      ['team.id'],
      metadataGetter,
      'postgres',
      undefined,
      { team: entry },
    );
    expect(result.batchFetchDescriptors[0]?.fields).toEqual(['id']);
    expect(result.batchFetchDescriptors[0]?.userLimit).toBeUndefined();
  });

  it('rejects owner relations whose foreign key is absent from metadata', async () => {
    const invalidMetadata = {
      ...metadata,
      relations: [
        {
          propertyName: 'team',
          type: 'many-to-one' as const,
          targetTableName: 'teams',
          foreignKeyColumn: 'missingTeamId',
        },
      ],
    };
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['team.id'],
        async (tableName) =>
          tableName === 'users' ? invalidMetadata : null,
        'postgres',
      ),
    ).rejects.toThrow(/foreign key.*metadata/i);
  });

  it('does not infer a non-primary id column for inverse correlation', async () => {
    const noPrimaryMetadata = {
      ...metadata,
      columns: [
        { name: 'id', type: 'uuid', isPrimary: false },
        { name: 'name', type: 'varchar' },
      ],
      relations: [
        {
          propertyName: 'children',
          type: 'one-to-many' as const,
          targetTableName: 'children',
          foreignKeyColumn: 'userId',
        },
      ],
    };
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['children.id'],
        async (tableName) =>
          tableName === 'users' ? noPrimaryMetadata : null,
        'postgres',
      ),
    ).rejects.toThrow(/parent primary key metadata/i);
  });

  it('rejects owner relation aliases that collide with selected columns', async () => {
    const collidingMetadata = {
      name: 'users',
      columns: [
        { name: 'id', type: 'uuid', isPrimary: true },
        { name: 'team', type: 'varchar' },
        { name: 'teamId', type: 'uuid' },
      ],
      relations: [
        {
          propertyName: 'team',
          type: 'many-to-one' as const,
          targetTableName: 'teams',
          foreignKeyColumn: 'teamId',
        },
      ],
    };
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['team', 'team.id'],
        async (tableName) =>
          tableName === 'users' ? collidingMetadata : null,
        'postgres',
      ),
    ).rejects.toThrow(/conflicts with a selected root column/i);
  });

  it('quotes scalar projection identifiers for the selected dialect', async () => {
    const result = await expandFieldsToJoinsAndSelect(
      'users',
      ['name'],
      metadataGetter,
      'postgres',
    );
    expect(result.select).toEqual(['"users"."name"']);
  });

  it('rejects one-to-one relation expansion without ownership metadata', async () => {
    const ambiguousMetadata = {
      ...metadata,
      relations: [
        {
          propertyName: 'profile',
          type: 'one-to-one' as const,
          targetTableName: 'profiles',
        },
      ],
    };
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['profile.id'],
        async (tableName) =>
          tableName === 'users' ? ambiguousMetadata : null,
        'postgres',
      ),
    ).rejects.toThrow(/ownership metadata/i);
  });

  it('retains the resolved owner foreign key in the batch descriptor', async () => {
    const result = await expandFieldsToJoinsAndSelect(
      'users',
      ['team.id'],
      metadataGetter,
      'postgres',
    );
    expect(result.batchFetchDescriptors[0]?.fkColumn).toBe('teamId');
    expect(result.batchFetchDescriptors[0]?.injectedParentFields).toEqual([
      'team',
    ]);
  });

  it.each([null, false, '', ' '])(
    'rejects coercible invalid relation limits %#',
    async (limit) => {
      await expect(
        expandFieldsToJoinsAndSelect(
          'users',
          ['team.id'],
          metadataGetter,
          'postgres',
          undefined,
          { team: { limit } },
        ),
      ).rejects.toThrow('relation limit');
    },
  );

  it.each([null, false, true, '', ' '])(
    'rejects coercible invalid relation pages %#',
    async (page) => {
      await expect(
        expandFieldsToJoinsAndSelect(
          'users',
          ['team.id'],
          metadataGetter,
          'postgres',
          undefined,
          { team: { page } },
        ),
      ).rejects.toThrow('relation page');
    },
  );

  it('normalizes and validates array-valued deep relation fields', async () => {
    const result = await expandFieldsToJoinsAndSelect(
      'users',
      ['team.id'],
      metadataGetter,
      'postgres',
      undefined,
      { team: { fields: [' id ', '', 'name'] } },
    );
    expect(result.batchFetchDescriptors[0]?.fields).toEqual(['id', 'name']);

    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['team.id'],
        metadataGetter,
        'postgres',
        undefined,
        { team: { fields: ['id', null] } },
      ),
    ).rejects.toThrow(/relation fields.*strings/i);
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['team.id'],
        metadataGetter,
        'postgres',
        undefined,
        { team: { fields: 123 } },
      ),
    ).rejects.toThrow(/relation fields.*string/i);
  });

  it('strips unknown relation paths before applying the depth guard', async () => {
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['missing.id'],
        metadataGetter,
        'postgres',
        0,
      ),
    ).resolves.toMatchObject({ select: [], batchFetchDescriptors: [] });
  });

  it('uses target primary keys for implicit relation fields', async () => {
    const customTargetMetadata = {
      ...metadata,
      relations: [
        {
          propertyName: 'team',
          type: 'many-to-one' as const,
          targetTableName: 'teams',
        },
      ],
    };
    const result = await expandFieldsToJoinsAndSelect(
      'users',
      ['team'],
      async (tableName) => {
        if (tableName === 'users') return customTargetMetadata;
        if (tableName === 'teams') {
          return {
            name: 'teams',
            columns: [
              { name: 'tenantId', type: 'uuid', isPrimary: true },
              { name: 'code', type: 'varchar', isPrimary: true },
            ],
            relations: [],
          };
        }
        return null;
      },
      'postgres',
    );
    expect(result.batchFetchDescriptors[0]?.fields).toEqual([
      'tenantId',
      'code',
    ]);
  });

  it('injects an omitted parent primary key for inverse correlation', async () => {
    const inverseMetadata = {
      ...metadata,
      relations: [
        {
          propertyName: 'children',
          type: 'one-to-many' as const,
          targetTableName: 'children',
          foreignKeyColumn: 'userId',
        },
      ],
    };
    const result = await expandFieldsToJoinsAndSelect(
      'users',
      ['name', 'children.name'],
      async (tableName) => (tableName === 'users' ? inverseMetadata : null),
      'postgres',
    );
    expect(result.select).toContain('"users"."id"');
    expect(result.batchFetchDescriptors[0]?.injectedParentFields).toEqual([
      'id',
    ]);
  });

  it('rejects missing table metadata', async () => {
    await expect(
      expandFieldsToJoinsAndSelect(
        'missing',
        ['id'],
        async () => null,
        'postgres',
      ),
    ).rejects.toThrow(/requires metadata/i);
  });

  it('rejects relation expansion when the query depth is exhausted', async () => {
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['team.id'],
        metadataGetter,
        'postgres',
        0,
      ),
    ).rejects.toThrow(/maximum query depth/i);
  });

  it('rejects invalid deep pagination before creating a descriptor', async () => {
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['team.id'],
        metadataGetter,
        'postgres',
        undefined,
        { team: { limit: 'invalid' } },
      ),
    ).rejects.toThrow('relation limit');
    await expect(
      expandFieldsToJoinsAndSelect(
        'users',
        ['team.id'],
        metadataGetter,
        'postgres',
        undefined,
        { team: { page: 0 } },
      ),
    ).rejects.toThrow('relation page');
  });

  it.each([
    { propertyName: 'team', targetTableName: 'teams' },
    {
      propertyName: 'profile',
      type: 'one-to-one',
      targetTableName: 'profiles',
    },
    {
      propertyName: 'items',
      type: 'unsupported',
      targetTableName: 'items',
    },
  ])('rejects ambiguous relation-sort metadata %#', (relationMeta) => {
    expect(() =>
      buildRelationSortSubquery(
        relationMeta,
        'name',
        'users',
        {
          tables: new Map([
            [
              relationMeta.targetTableName,
              {
                columns: [
                  { name: 'id', isPrimary: true },
                  { name: 'name' },
                ],
              },
            ],
          ]),
        },
        'postgres',
      ),
    ).toThrow(/explicitly parent-owned to-one/i);
  });

  it('aliases self-referential relation sort subqueries', () => {
    const sql = buildRelationSortSubquery(
      {
        propertyName: 'manager',
        type: 'many-to-one',
        targetTableName: 'employees',
        foreignKeyColumn: 'managerId',
      },
      'name',
      'employees',
      {
        tables: new Map([
          [
            'employees',
            {
              columns: [
                { name: 'id', isPrimary: true },
                { name: 'name' },
                { name: 'managerId' },
              ],
            },
          ],
        ]),
      },
      'postgres',
    );
    expect(sql).toContain('FROM "employees" AS "__relation_sort_target"');
    expect(sql).toContain(
      '"__relation_sort_target"."id" = "employees"."managerId"',
    );
  });

  it('keeps a self-relation sort alias distinct from a matching outer table name', () => {
    const table = '__relation_sort_target';
    const sql = buildRelationSortSubquery(
      { propertyName: 'manager', type: 'many-to-one', targetTableName: table, foreignKeyColumn: 'managerId' },
      'name',
      table,
      { tables: new Map([[table, { columns: [
        { name: 'id', isPrimary: true }, { name: 'name' }, { name: 'managerId' },
      ] }]]) },
      'postgres',
    );

    expect(sql).toContain('AS "__relation_sort_target_1"');
    expect(sql).toContain('"__relation_sort_target_1"."id" = "__relation_sort_target"."managerId"');
  });

  it('rejects inverse relation sort subqueries', () => {
    expect(() =>
      buildRelationSortSubquery(
        {
          propertyName: 'profile',
          type: 'one-to-one',
          isInverse: true,
          targetTableName: 'profiles',
          foreignKeyColumn: 'userId',
        },
        'name',
        'users',
        {
          tables: new Map([
            [
              'profiles',
              {
                columns: [
                  { name: 'id', isPrimary: true },
                  { name: 'name' },
                ],
              },
            ],
          ]),
        },
        'postgres',
      ),
    ).toThrow(/parent-owned to-one/i);
  });

  it('rejects relation-sort columns missing from metadata', () => {
    const relationMeta = {
      propertyName: 'team',
      type: 'many-to-one',
      targetTableName: 'teams',
      foreignKeyColumn: 'teamId',
    };
    const relationSortMetadata = {
      tables: new Map([
        [
          'users',
          {
            columns: [
              { name: 'id', isPrimary: true },
              { name: 'teamId' },
            ],
          },
        ],
        [
          'teams',
          {
            columns: [
              { name: 'id', isPrimary: true },
              { name: 'name' },
            ],
          },
        ],
      ]),
    };

    expect(() =>
      buildRelationSortSubquery(
        relationMeta,
        'missing',
        'users',
        relationSortMetadata,
        'postgres',
      ),
    ).toThrow(/target column/i);
    expect(() =>
      buildRelationSortSubquery(
        { ...relationMeta, foreignKeyColumn: 'missingTeamId' },
        'name',
        'users',
        relationSortMetadata,
        'postgres',
      ),
    ).toThrow(/foreign-key column/i);
  });

  it('fails closed when relation-sort metadata is incomplete', () => {
    expect(() =>
      buildRelationSortSubquery(
        {
          propertyName: 'team',
          type: 'many-to-one',
          targetTableName: 'teams',
          foreignKeyColumn: 'teamId',
        },
        'name',
        'users',
        { tables: new Map() },
        'postgres',
      ),
    ).toThrow('target metadata');
  });

  it('propagates expansion failures instead of silently changing the selection', async () => {
    await expect(
      expandFieldsToSelect(
        {} as never,
        'users',
        ['team.id'],
        metadataGetter,
        'postgres',
        undefined,
        { team: { limit: 'invalid' } },
      ),
    ).rejects.toThrow('relation limit');
  });
});
