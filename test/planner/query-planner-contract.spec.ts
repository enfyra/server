import { describe, expect, it } from 'vitest';
import {
  ALL_SUPPORTED_OPERATORS,
  FIELD_OPERATORS,
  LOGICAL_OPERATORS,
  QueryPlanner,
} from '@enfyra/kernel';

const metadata = {
  tables: new Map([
    [
      'posts',
      {
        name: 'posts',
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'title', type: 'varchar' },
        ],
        relations: [
          {
            propertyName: 'comments',
            type: 'one-to-many',
            targetTableName: 'comments',
            foreignKeyColumn: 'postId',
          },
        ],
      },
    ],
    [
      'comments',
      {
        name: 'comments',
        columns: [
          { name: 'id', type: 'integer', isPrimary: true },
          { name: 'postId', type: 'integer' },
        ],
        relations: [],
      },
    ],
    [
      'mongoRows',
      {
        name: 'mongoRows',
        columns: [{ name: '_id', type: 'objectid', isPrimary: true }],
        relations: [],
      },
    ],
    [
      'compositeRows',
      {
        name: 'compositeRows',
        columns: [
          { name: 'tenantId', type: 'integer', isPrimary: true },
          { name: 'sequence', type: 'integer', isPrimary: true },
        ],
        relations: [],
      },
    ],
    [
      'noPrimaryKey',
      {
        name: 'noPrimaryKey',
        columns: [{ name: 'name', type: 'varchar' }],
        relations: [],
      },
    ],
    [
      'encryptedPrimary',
      {
        name: 'encryptedPrimary',
        columns: [
          { name: 'id', type: 'integer', isPrimary: true, isEncrypted: true },
        ],
        relations: [],
      },
    ],
    [
      'mongoParents',
      {
        name: 'mongoParents',
        columns: [{ name: '_id', type: 'objectid', isPrimary: true }],
        relations: [
          {
            propertyName: 'children',
            type: 'one-to-many',
            targetTableName: 'encryptedMongoRows',
          },
        ],
      },
    ],
    [
      'encryptedMongoRows',
      {
        name: 'encryptedMongoRows',
        columns: [
          {
            name: '_id',
            type: 'objectid',
            isPrimary: true,
            isEncrypted: true,
          },
        ],
        relations: [],
      },
    ],
  ]),
};

function plan(overrides: Record<string, any> = {}) {
  return new QueryPlanner().plan({
    tableName: 'posts',
    metadata,
    dbType: 'postgres',
    ...overrides,
  });
}

describe('QueryPlanner contracts', () => {
  it('exposes immutable operator registries', () => {
    expect(() => (FIELD_OPERATORS as Set<string>).delete('_eq')).toThrow();
    expect(() => (LOGICAL_OPERATORS as Set<string>).add('_xor')).toThrow();
    expect(() => (ALL_SUPPORTED_OPERATORS as string[]).push('_xor')).toThrow();
    expect(() =>
      FIELD_OPERATORS.forEach((_value, _duplicate, set) => {
        Set.prototype.add.call(set, '_xor');
      }),
    ).toThrow();
    expect(FIELD_OPERATORS.has('_eq')).toBe(true);
    expect(ALL_SUPPORTED_OPERATORS).not.toContain('_xor');
  });

  it('marks aggregate relation sorts as relation sorts', () => {
    const result = plan({ sort: '_count(comments)' });
    expect(result.hasRelationSort).toBe(true);
    expect(result.sortItems[0]?.relationName).toBe('comments');
  });

  it('prefers an exact Mongo id column over the _id alias', () => {
    const mongoWithExactId = {
      tables: new Map([
        [
          'rows',
          {
            name: 'rows',
            columns: [
              { name: '_id', type: 'objectid', isPrimary: true },
              { name: 'id', type: 'varchar' },
            ],
            relations: [],
          },
        ],
      ]),
    };
    const result = new QueryPlanner().plan({
      tableName: 'rows',
      metadata: mongoWithExactId,
      dbType: 'mongodb',
      sort: 'id',
    });
    expect(result.sortItems[0]?.field).toBe('id');
  });

  it('uses the metadata-backed Mongo primary key for implicit sorting', () => {
    const result = plan({ tableName: 'mongoRows', dbType: 'mongodb' });
    expect(result.sortItems).toEqual([
      { joinId: null, field: '_id', direction: 'asc', fullPath: '_id' },
    ]);
  });

  it('uses every composite primary-key column for implicit sorting', () => {
    expect(plan({ tableName: 'compositeRows' }).sortItems).toEqual([
      {
        joinId: null,
        field: 'tenantId',
        direction: 'asc',
        fullPath: 'tenantId',
      },
      {
        joinId: null,
        field: 'sequence',
        direction: 'asc',
        fullPath: 'sequence',
      },
    ]);
  });

  it('fails closed when filters or sorts require missing metadata', () => {
    expect(() =>
      new QueryPlanner().plan({
        tableName: 'missing',
        metadata,
        dbType: 'postgres',
        filter: { id: { _eq: 1 } },
      }),
    ).toThrow(/metadata/i);
    expect(() =>
      new QueryPlanner().plan({
        tableName: 'missing',
        metadata,
        dbType: 'postgres',
        sort: 'id',
      }),
    ).toThrow(/metadata/i);
  });

  it('normalizes array metadata selectors', () => {
    const result = plan({ meta: [' totalCount ', '', ' filterCount '] });
    expect(result.needsTotalCount).toBe(true);
    expect(result.needsFilterCount).toBe(true);
  });

  it('does not invent an implicit sort when no primary key is available', () => {
    expect(plan({ tableName: 'noPrimaryKey' }).sortItems).toEqual([]);
  });

  it('rejects an encrypted implicit primary-key sort', () => {
    expect(() => plan({ tableName: 'encryptedPrimary' })).toThrow(
      /encrypted field.*sort/i,
    );
  });

  it('rejects an encrypted Mongo id alias in relation aggregate sort', () => {
    expect(() =>
      plan({
        tableName: 'mongoParents',
        dbType: 'mongodb',
        sort: '_max(children.id)',
      }),
    ).toThrow(/encrypted field.*sort/i);
  });

  it('appends missing primary-key tie-breakers after an explicit sort', () => {
    expect(plan({ sort: '-title', page: 2, limit: 10 }).sortItems).toEqual([
      { joinId: null, field: 'title', direction: 'desc', fullPath: 'title' },
      { joinId: null, field: 'id', direction: 'asc', fullPath: 'id' },
    ]);
  });

  it('rejects pagination when no deterministic primary key exists', () => {
    expect(() =>
      plan({ tableName: 'noPrimaryKey', page: 2, limit: 10 }),
    ).toThrow(/primary key|deterministic/i);
  });

  it('rejects page without limit', () => {
    expect(() => plan({ page: 2 })).toThrow(/page.*limit/i);
  });

  it('does not treat undeclared id columns as deterministic pagination keys', () => {
    const unkeyedIdMetadata = {
      tables: new Map([
        [
          'rows',
          {
            name: 'rows',
            columns: [
              { name: 'id', type: 'integer', isPrimary: false },
              { name: 'title', type: 'varchar' },
            ],
            relations: [],
          },
        ],
      ]),
    };
    expect(() =>
      new QueryPlanner().plan({
        tableName: 'rows',
        metadata: unkeyedIdMetadata,
        dbType: 'postgres',
        page: 2,
        limit: 10,
      }),
    ).toThrow(/primary key|deterministic/i);
  });

  it.each([
    { limit: '12x' },
    { limit: 1.5 },
    { limit: Number.POSITIVE_INFINITY },
    { limit: Number.MAX_SAFE_INTEGER + 1 },
    { limit: -1 },
    { page: '2x', limit: 10 },
    { page: 1.5, limit: 10 },
    { page: 0, limit: 10 },
    { page: Number.MAX_SAFE_INTEGER, limit: 2 },
  ])('rejects non-canonical pagination %#', (pagination) => {
    expect(() => plan(pagination)).toThrow(/pagination|limit|page/i);
  });
});
