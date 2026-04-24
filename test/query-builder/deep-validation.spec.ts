import { validateDeepOptions } from '../../src/infrastructure/query-builder/utils/shared/deep-options-validator.util';

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

  test('rejects invalid sort field', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { sort: 'nonexistentColumn' } },
        metadata,
      ),
    ).toThrow(/references unknown field 'nonexistentColumn'/);
  });

  test('accepts valid dotted sort through m2o chain', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { author: { sort: 'company.name' } },
        metadata,
      ),
    ).not.toThrow();
  });

  test('rejects dotted sort through o2m relation', () => {
    expect(() =>
      validateDeepOptions(
        'users',
        { posts: { sort: 'comments.body', limit: 3 } },
        metadata,
      ),
    ).toThrow(/one-to-many.*sort path must only traverse/i);
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
    ).toThrow(/exceeds maximum query depth/i);
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

  test('accepts dotted sort at max 3 hops (author.company.region.name)', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { sort: 'post.author.company.name' } },
        metadata,
      ),
    ).not.toThrow();
  });

  test('rejects dotted sort exceeding 3 hops', () => {
    expect(() =>
      validateDeepOptions(
        'posts',
        { comments: { sort: 'post.author.company.region.name' } },
        metadata,
      ),
    ).toThrow(/exceeds max dotted hops of 3/i);
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
