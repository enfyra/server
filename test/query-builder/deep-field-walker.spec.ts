import {
  rewriteFilterDenyingFields,
  rewriteSortDroppingDenied,
} from '@enfyra/kernel';

const META: Record<string, any> = {
  posts: {
    name: 'posts',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'title', type: 'varchar' },
      { name: 'secret', type: 'varchar' },
      { name: 'authorId', type: 'integer' },
    ],
    relations: [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: 'users',
        targetTable: 'users',
      },
      {
        propertyName: 'privateTag',
        type: 'many-to-one',
        targetTableName: 'tags',
        targetTable: 'tags',
      },
    ],
  },
  users: {
    name: 'users',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'varchar' },
      { name: 'internalNote', type: 'varchar' },
    ],
    relations: [],
  },
  tags: {
    name: 'tags',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'label', type: 'varchar' },
    ],
    relations: [],
  },
};

const metadata = { tables: new Map(Object.entries(META)) };

function denyField(tableName: string, fieldName: string) {
  return (tbl: string, field: string) =>
    !(tbl === tableName && field === fieldName);
}

function allowAll() {
  return () => true;
}

describe('rewriteFilterDenyingFields', () => {
  test('rejects misplaced field operators at table scope', () => {
    expect(() =>
      rewriteFilterDenyingFields(
        { _eq: 1 },
        'posts',
        metadata,
        allowAll(),
      ),
    ).toThrow(/operator|unknown filter field/i);
  });

  test('rejects circular allowed-column operands', () => {
    const circular: any = [];
    circular.push(circular);
    expect(() =>
      rewriteFilterDenyingFields(
        { title: { _in: circular } },
        'posts',
        metadata,
        allowAll(),
      ),
    ).toThrow(/circular/i);
  });

  test('passes through filter when all allowed', () => {
    const filter = { title: { _eq: 'hello' } };
    const result = rewriteFilterDenyingFields(
      filter,
      'posts',
      metadata,
      allowAll(),
    );
    expect(result).toEqual(filter);
  });

  test('rejects denied scalar fields instead of broadening the filter', () => {
    const filter = { secret: { _eq: 'x' }, title: { _eq: 'y' } };
    expect(() =>
      rewriteFilterDenyingFields(
        filter,
        'posts',
        metadata,
        denyField('posts', 'secret'),
      ),
    ).toThrow(/not allowed/i);
  });

  test('rejects denied relation subtrees instead of broadening the filter', () => {
    const filter = {
      title: { _eq: 'a' },
      privateTag: { label: { _eq: 'private' } },
    };
    expect(() =>
      rewriteFilterDenyingFields(
        filter,
        'posts',
        metadata,
        denyField('posts', 'privateTag'),
      ),
    ).toThrow(/not allowed/i);
  });

  test('rejects denied fields inside nested relations', () => {
    const filter = {
      author: {
        name: { _contains: 'Alice' },
        internalNote: { _eq: 'secret' },
      },
    };
    expect(() =>
      rewriteFilterDenyingFields(
        filter,
        'posts',
        metadata,
        denyField('users', 'internalNote'),
      ),
    ).toThrow(/not allowed/i);
  });

  test('rejects fully denied nested relation filters', () => {
    const filter = {
      author: {
        internalNote: { _eq: 'secret' },
      },
    };
    expect(() =>
      rewriteFilterDenyingFields(
        filter,
        'posts',
        metadata,
        denyField('users', 'internalNote'),
      ),
    ).toThrow(/not allowed/i);
  });

  test('rejects denied fields in _and arrays', () => {
    const filter = {
      _and: [{ secret: { _eq: 'x' } }, { title: { _eq: 'y' } }],
    };
    expect(() =>
      rewriteFilterDenyingFields(
        filter,
        'posts',
        metadata,
        denyField('posts', 'secret'),
      ),
    ).toThrow(/not allowed/i);
  });

  test('rejects fully denied _and arrays', () => {
    const filter = {
      _and: [{ secret: { _eq: 'x' } }],
    };
    expect(() =>
      rewriteFilterDenyingFields(
        filter,
        'posts',
        metadata,
        denyField('posts', 'secret'),
      ),
    ).toThrow(/not allowed/i);
  });

  test('rejects denied fields in _or arrays', () => {
    const filter = {
      _or: [{ secret: { _eq: 'x' } }, { title: { _eq: 'y' } }],
    };
    expect(() =>
      rewriteFilterDenyingFields(
        filter,
        'posts',
        metadata,
        denyField('posts', 'secret'),
      ),
    ).toThrow(/not allowed/i);
  });

  test('rejects fields absent from metadata', () => {
    expect(() =>
      rewriteFilterDenyingFields(
        { missing: { _eq: 'x' }, title: { _eq: 'y' } },
        'posts',
        metadata,
        allowAll(),
      ),
    ).toThrow(/unknown filter field/i);
  });

  test('rejects prototype-mutating filter keys', () => {
    const filter = JSON.parse('{"__proto__":{"polluted":true},"title":{"_eq":"y"}}');
    expect(() =>
      rewriteFilterDenyingFields(filter, 'posts', metadata, allowAll()),
    ).toThrow(/unknown filter field/i);
    expect(({} as any).polluted).toBeUndefined();
  });

  test('rejects a root filter when every predicate is denied', () => {
    const filter = { secret: { _eq: 'x' } };
    expect(() =>
      rewriteFilterDenyingFields(
        filter,
        'posts',
        metadata,
        denyField('posts', 'secret'),
      ),
    ).toThrow(/not allowed/i);
  });
});

describe('rewriteSortDroppingDenied', () => {
  test('passes through when all allowed', () => {
    const result = rewriteSortDroppingDenied(
      'title,-id',
      'posts',
      metadata,
      allowAll(),
    );
    expect(result).toBe('title,-id');
  });

  test('drops denied scalar field token', () => {
    const result = rewriteSortDroppingDenied(
      'secret,-title',
      'posts',
      metadata,
      denyField('posts', 'secret'),
    );
    expect(result).toBe('-title');
  });

  test('drops all tokens if all denied → returns undefined', () => {
    const result = rewriteSortDroppingDenied(
      'secret',
      'posts',
      metadata,
      denyField('posts', 'secret'),
    );
    expect(result).toBeUndefined();
  });

  test('drops token when relation in path denied', () => {
    const result = rewriteSortDroppingDenied(
      'author.name',
      'posts',
      metadata,
      denyField('posts', 'author'),
    );
    expect(result).toBeUndefined();
  });

  test('drops token when leaf field in nested path denied', () => {
    const result = rewriteSortDroppingDenied(
      'author.internalNote',
      'posts',
      metadata,
      denyField('users', 'internalNote'),
    );
    expect(result).toBeUndefined();
  });

  test('keeps allowed token from dotted path', () => {
    const result = rewriteSortDroppingDenied(
      '-author.name',
      'posts',
      metadata,
      allowAll(),
    );
    expect(result).toBe('-author.name');
  });

  test('rejects unknown sort leaves', () => {
    expect(() =>
      rewriteSortDroppingDenied(
        'missing',
        'posts',
        metadata,
        allowAll(),
      ),
    ).toThrow(/unknown sort field/i);
  });

  test('rejects terminal relation sort tokens', () => {
    expect(() =>
      rewriteSortDroppingDenied('author', 'posts', metadata, allowAll()),
    ).toThrow(/unknown sort field/i);
  });

  test('rejects sort paths whose target metadata is unavailable', () => {
    const missingTargetMetadata = {
      tables: new Map([
        [
          'posts',
          {
            ...META.posts,
            relations: [
              {
                propertyName: 'author',
                type: 'many-to-one',
                targetTableName: 'missing_users',
              },
            ],
          },
        ],
      ]),
    };
    expect(() =>
      rewriteSortDroppingDenied(
        'author.name',
        'posts',
        missingTargetMetadata,
        allowAll(),
      ),
    ).toThrow(/metadata|unknown table/i);
  });

  test('array input returns array', () => {
    const result = rewriteSortDroppingDenied(
      ['secret', 'title'],
      'posts',
      metadata,
      denyField('posts', 'secret'),
    );
    expect(Array.isArray(result)).toBe(true);
    expect(result).toEqual(['title']);
  });
});
