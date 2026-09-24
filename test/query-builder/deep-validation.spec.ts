import { validateDeepOptions } from '@enfyra/kernel';

const META: Record<string, any> = {
  posts: {
    name: 'posts',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'title', type: 'varchar' },
      { name: 'isPublished', type: 'boolean' },
      { name: 'authorId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: 'users',
        targetTable: 'users',
        isInverse: false,
      },
      {
        propertyName: 'comments',
        type: 'one-to-many',
        targetTableName: 'comments',
        targetTable: 'comments',
        isInverse: true,
        mappedBy: 'post',
      },
      {
        propertyName: 'tags',
        type: 'many-to-many',
        targetTableName: 'tags',
        targetTable: 'tags',
        isInverse: false,
        junctionTableName: 'posts_tags',
        junctionSourceColumn: 'postId',
        junctionTargetColumn: 'tagId',
      },
    ],
  },
  users: {
    name: 'users',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
      { name: 'companyId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'company',
        type: 'many-to-one',
        targetTableName: 'companies',
        targetTable: 'companies',
        isInverse: false,
      },
      {
        propertyName: 'posts',
        type: 'one-to-many',
        targetTableName: 'posts',
        targetTable: 'posts',
        isInverse: true,
        mappedBy: 'author',
      },
    ],
  },
  comments: {
    name: 'comments',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'body', type: 'varchar' },
      { name: 'isPublished', type: 'boolean' },
      { name: 'postId', type: 'integer' },
      { name: 'createdAt', type: 'timestamp' },
    ],
    relations: [
      {
        propertyName: 'post',
        type: 'many-to-one',
        targetTableName: 'posts',
        targetTable: 'posts',
        isInverse: false,
      },
    ],
  },
  companies: {
    name: 'companies',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
      { name: 'regionId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'region',
        type: 'many-to-one',
        targetTableName: 'regions',
        targetTable: 'regions',
        isInverse: false,
      },
    ],
  },
  regions: {
    name: 'regions',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
      { name: 'countryId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'country',
        type: 'many-to-one',
        targetTableName: 'countries',
        targetTable: 'countries',
        isInverse: false,
      },
    ],
  },
  countries: {
    name: 'countries',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'name', type: 'varchar' },
    ],
    relations: [],
  },
  tags: {
    name: 'tags',
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'label', type: 'varchar' },
    ],
    relations: [],
  },
};

const metadata = { tables: new Map(Object.entries(META)) };

describe('validateDeepOptions', () => {
  test('passes for valid o2m deep with filter + sort + limit', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          comments: {
            filter: { isPublished: { _eq: true } },
            sort: '-createdAt',
            limit: 3,
          },
        },
        metadata,
      ),
    ).not.toThrow();
  });

  test('passes for valid m2o deep with filter + sort (no limit)', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          author: {
            filter: { name: { _contains: 'Alice' } },
            sort: 'name',
          },
        },
        metadata,
      ),
    ).not.toThrow();
  });

  test('rejects limit on many-to-one', () => {
    expect(() =>
      validateDeepOptions('posts', { author: { limit: 5 } }, metadata),
    ).toThrow(/limit.*not supported.*many-to-one/i);
  });

  test('rejects limit on one-to-one owner', () => {
    const metaWithO2O = {
      tables: new Map([
        ...metadata.tables.entries(),
        [
          'profiles',
          {
            name: 'profiles',
            columns: [{ name: 'id', type: 'integer' }],
            relations: [
              {
                propertyName: 'user',
                type: 'one-to-one',
                targetTableName: 'users',
                targetTable: 'users',
                isInverse: false,
              },
            ],
          },
        ],
      ]),
    };
    expect(() =>
      validateDeepOptions('profiles', { user: { limit: 1 } }, metaWithO2O),
    ).toThrow(/limit.*not supported/i);
  });

  test('rejects unknown relation key', () => {
    expect(() =>
      validateDeepOptions('posts', { nonexistent: { filter: {} } }, metadata),
    ).toThrow(/Unknown relation 'nonexistent'/);
  });

  test.each([null, 42, [], 'invalid'])('rejects malformed deep relation option: %p', (entry) => {
    expect(() =>
      validateDeepOptions('posts', { comments: entry as any }, metadata),
    ).toThrow(/deep option.*object/i);
  });

  test('rejects unknown sub-key in deep entry', () => {
    expect(() =>
      validateDeepOptions('posts', { comments: { invalidKey: 123 } }, metadata),
    ).toThrow(/Unknown deep option key 'invalidKey'/);
  });

  test('rejects page without limit', () => {
    expect(() =>
      validateDeepOptions('posts', { comments: { page: 2 } }, metadata),
    ).toThrow(/page.*requires.*limit/i);
  });

  test('accepts page with limit', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { limit: 5, page: 2 } },
        metadata,
      ),
    ).not.toThrow();
  });

  test.each([
    { limit: -1 },
    { limit: 1.5 },
    { limit: '5' },
    { page: 0, limit: 5 },
    { page: 1.5, limit: 5 },
    { page: '2', limit: 5 },
  ])('rejects malformed deep pagination %#', (entry) => {
    expect(() =>
      validateDeepOptions('posts', { comments: entry }, metadata),
    ).toThrow(/limit|page/i);
  });

  test.each([null, false, ['id', 7], {}, 7])(
    'rejects malformed deep fields: %p',
    (fields) => {
      expect(() =>
        validateDeepOptions(
          'posts',
          { comments: { fields } },
          metadata,
        ),
      ).toThrow(/fields/i);
    },
  );

  test('strips unknown deep fields without rejecting the deep options', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { fields: ['id', 'missing'] } },
        metadata,
      ),
    ).not.toThrow();
  });

  test('strips unresolved segments of dotted deep fields', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { fields: ['post.author.missing', 'body.missing'] } },
        metadata,
      ),
    ).not.toThrow();
  });

  test('validates the terminal segment of dotted filter keys', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          comments: {
            filter: { 'post.author.missing': { _eq: 'x' } },
          },
        },
        metadata,
      ),
    ).toThrow(/missing|unknown/i);
  });

  test.each([
    { filter: false },
    { sort: 0 },
    { deep: null },
    { deep: [] },
  ])('rejects malformed falsey deep option %#', (entry) => {
    expect(() =>
      validateDeepOptions('posts', { comments: entry }, metadata),
    ).toThrow(/filter|sort|deep/i);
  });

  test('rejects relations without resolvable target metadata', () => {
    const incompleteMetadata = {
      tables: new Map([
        [
          'posts',
          {
            ...META.posts,
            relations: [
              {
                propertyName: 'comments',
                type: 'one-to-many',
                targetTableName: 'missing_comments',
              },
            ],
          },
        ],
      ]),
    };
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { filter: { id: { _eq: 1 } } } },
        incompleteMetadata,
      ),
    ).toThrow(/target table|metadata/i);
  });

  test('rejects encrypted guard paths with incomplete metadata', () => {
    const incompleteMetadata = {
      tables: new Map([
        [
          'posts',
          {
            ...META.posts,
            relations: [
              {
                propertyName: 'comments',
                type: 'one-to-many',
                targetTableName: 'missing_comments',
              },
            ],
          },
        ],
      ]),
    };
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { sort: 'id' } },
        incompleteMetadata,
      ),
    ).toThrow(/target table|metadata/i);
  });

  test('handles shared operand graphs without exponential traversal', () => {
    let shared: any = 'leaf';
    for (let index = 0; index < 45; index += 1) {
      shared = { left: shared, right: shared };
    }
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { filter: { body: { _eq: shared } } } },
        metadata,
      ),
    ).not.toThrow();
  });

  test('rejects invalid sort field', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { sort: 'nonexistentColumn' } },
        metadata,
      ),
    ).toThrow(/does not exist/i);
  });

  test('rejects dotted sort through m2o chain', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { author: { sort: 'company.name' } },
        metadata,
      ),
    ).toThrow(/Sort field 'company\.name' is not supported/i);
  });

  test('rejects dotted sort through o2m relation', () => {
    expect(() =>
      validateDeepOptions(
        'users',
        { posts: { sort: 'comments.body', limit: 3 } },
        metadata,
      ),
    ).toThrow(/Sort field 'comments\.body' is not supported/i);
  });

  test('validates nested deep recursively', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          comments: {
            filter: { isPublished: { _eq: true } },
            deep: {
              nonexistent: {},
            },
          },
        },
        metadata,
      ),
    ).toThrow(/Unknown relation 'nonexistent'/);
  });

  test('max depth enforcement', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { filter: {} } },
        metadata,
        0,
        0,
      ),
    ).toThrow(/recursion bounds|exceeds maximum query depth/i);
  });

  test('rejects invalid filter shape in deep', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          comments: {
            filter: { isPublished: { _unsupportedOp: true } },
          },
        },
        metadata,
      ),
    ).toThrow(/Unsupported filter operator/i);
  });

  test('accepts m2m with limit', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { tags: { limit: 5, sort: 'label' } },
        metadata,
      ),
    ).not.toThrow();
  });

  test('rejects dotted sort at any depth', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { sort: 'post.author.company.name' } },
        metadata,
      ),
    ).toThrow(/Sort field 'post\.author\.company\.name' is not supported/i);
  });

  test('rejects dotted sort before hop-depth validation', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { sort: 'post.author.company.region.name' } },
        metadata,
      ),
    ).toThrow(/Sort field 'post\.author\.company\.region\.name' is not supported/i);
  });

  test('accepts nested filter at max 3 hops (post.author.company.name)', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          comments: {
            filter: {
              post: {
                author: {
                  company: { name: { _eq: 'Acme' } },
                },
              },
            },
          },
        },
        metadata,
      ),
    ).not.toThrow();
  });

  test('rejects dotted filter keys that exceed the relation hop limit', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          comments: {
            filter: {
              'post.author.company.region.name': { _eq: 'APAC' },
            },
          },
        },
        metadata,
      ),
    ).toThrow(/dotted hops/i);
  });

  test('rejects relation hops hidden behind logical groups', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          comments: {
            filter: {
              post: {
                _and: [
                  {
                    author: {
                      company: {
                        region: { name: { _eq: 'APAC' } },
                      },
                    },
                  },
                ],
              },
            },
          },
        },
        metadata,
      ),
    ).toThrow(/Filter path exceeds max dotted hops of 3/i);
  });

  test('rejects non-string sort array entries', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { sort: ['createdAt', 7] } },
        metadata,
      ),
    ).toThrow(/sort.*string/i);
  });

  test('rejects excessive logical nesting without overflowing the stack', () => {
    let filter: any = { isPublished: { _eq: true } };
    for (let index = 0; index < 300; index += 1) {
      filter = { _not: filter };
    }
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { filter } },
        metadata,
      ),
    ).toThrow(/nesting depth/i);
  });

  test('accepts underscore-prefixed fields declared on a nested relation target', () => {
    const mongoMetadata = {
      tables: new Map([
        ['posts', META.posts],
        [
          'comments',
          {
            ...META.comments,
            relations: [
              {
                propertyName: 'author',
                type: 'many-to-one',
                targetTableName: 'mongoUsers',
                targetTable: 'mongoUsers',
                isInverse: false,
              },
            ],
          },
        ],
        [
          'mongoUsers',
          {
            name: 'mongoUsers',
            columns: [{ name: '_id', type: 'objectid', isPrimary: true }],
            relations: [],
          },
        ],
      ]),
    };
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          comments: {
            filter: { author: { _id: { _eq: '507f1f77bcf86cd799439011' } } },
          },
        },
        mongoMetadata,
      ),
    ).not.toThrow();
  });

  test('rejects circular nesting inside field operands', () => {
    const circular: any = [];
    circular.push(circular);
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { filter: { body: { _in: circular } } } },
        metadata,
      ),
    ).toThrow(/circular/i);
  });

  test('rejects excessive nesting inside field operands', () => {
    let operand: any = 'value';
    for (let index = 0; index < 80; index += 1) operand = [operand];
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { filter: { body: { _eq: operand } } } },
        metadata,
      ),
    ).toThrow(/nesting depth/i);
  });

  test('rejects nested filter exceeding 3 hops', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        {
          comments: {
            filter: {
              post: {
                author: {
                  company: {
                    region: { name: { _eq: 'APAC' } },
                  },
                },
              },
            },
          },
        },
        metadata,
      ),
    ).toThrow(/Filter path exceeds max dotted hops of 3/i);
  });
});
