import {
  rewriteFilterDenyingFields,
  rewriteSortDroppingDenied,
} from '../../src/domain/query-dsl/filter-field-walker.util';

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

  test('strips denied scalar field', () => {
    const filter = { secret: { _eq: 'x' }, title: { _eq: 'y' } };
    const result = rewriteFilterDenyingFields(
      filter,
      'posts',
      metadata,
      denyField('posts', 'secret'),
    );
    expect(result.secret).toBeUndefined();
    expect(result.title).toEqual({ _eq: 'y' });
  });

  test('strips denied relation subtree', () => {
    const filter = {
      title: { _eq: 'a' },
      privateTag: { label: { _eq: 'private' } },
    };
    const result = rewriteFilterDenyingFields(
      filter,
      'posts',
      metadata,
      denyField('posts', 'privateTag'),
    );
    expect(result.privateTag).toBeUndefined();
    expect(result.title).toEqual({ _eq: 'a' });
  });

  test('strips denied field inside nested relation', () => {
    const filter = {
      author: {
        name: { _contains: 'Alice' },
        internalNote: { _eq: 'secret' },
      },
    };
    const result = rewriteFilterDenyingFields(
      filter,
      'posts',
      metadata,
      denyField('users', 'internalNote'),
    );
    expect(result.author).toBeDefined();
    expect(result.author.internalNote).toBeUndefined();
    expect(result.author.name).toEqual({ _contains: 'Alice' });
  });

  test('removes empty nested relation when all its fields denied', () => {
    const filter = {
      author: {
        internalNote: { _eq: 'secret' },
      },
    };
    const result = rewriteFilterDenyingFields(
      filter,
      'posts',
      metadata,
      denyField('users', 'internalNote'),
    );
    expect(result.author).toBeUndefined();
  });

  test('strips from _and array', () => {
    const filter = {
      _and: [{ secret: { _eq: 'x' } }, { title: { _eq: 'y' } }],
    };
    const result = rewriteFilterDenyingFields(
      filter,
      'posts',
      metadata,
      denyField('posts', 'secret'),
    );
    expect(result._and).toBeDefined();
    expect(result._and.length).toBe(1);
    expect(result._and[0].title).toEqual({ _eq: 'y' });
  });

  test('removes _and when all children stripped', () => {
    const filter = {
      _and: [{ secret: { _eq: 'x' } }],
    };
    const result = rewriteFilterDenyingFields(
      filter,
      'posts',
      metadata,
      denyField('posts', 'secret'),
    );
    expect(result._and).toBeUndefined();
  });

  test('strips from _or array', () => {
    const filter = {
      _or: [{ secret: { _eq: 'x' } }, { title: { _eq: 'y' } }],
    };
    const result = rewriteFilterDenyingFields(
      filter,
      'posts',
      metadata,
      denyField('posts', 'secret'),
    );
    expect(result._or).toBeDefined();
    expect(result._or.length).toBe(1);
  });

  test('returns empty object when root is all denied', () => {
    const filter = { secret: { _eq: 'x' } };
    const result = rewriteFilterDenyingFields(
      filter,
      'posts',
      metadata,
      denyField('posts', 'secret'),
    );
    expect(Object.keys(result).length).toBe(0);
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
