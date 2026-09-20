import { describe, expect, it } from 'vitest';
import {
  isSupportedMongoColumnType,
  isSupportedMySqlColumnType,
  isSupportedPostgresColumnType,
  toMongoTargetDataMigration,
  toMongoTargetMigration,
  toMongoTargetSnapshot,
  toSqlTargetSnapshot,
} from '../../src/shared/utils/column-type.util';
import { toMongoTypeForSqlType } from '../../src/shared/types/column-type.types';

function column(name: string, sqlType: string, extra: Record<string, unknown> = {}) {
  return {
    name,
    sqlType: { type: sqlType },
    mongoType: { type: toMongoTypeForSqlType(sqlType) },
    ...extra,
  };
}

describe('column type contract', () => {
  it('materializes SQL snapshot primitives as Mongo-native metadata types', () => {
    const target = toMongoTargetSnapshot({
      enfyra_column: {
        name: 'enfyra_column',
        columns: [
          {
            name: 'type',
            sqlType: { type: 'enum', options: ['varchar', 'simple-json'] },
            mongoType: {
              type: 'enum',
              options: ['string', 'json', 'objectId'],
            },
          },
        ],
      },
      example: {
        name: 'example',
        columns: [
          column('id', 'int', { isPrimary: true }),
          column('title', 'varchar'),
          column('enabled', 'boolean'),
          column('count', 'bigint'),
          column('score', 'float'),
          column('publishedAt', 'timestamp'),
          column('payload', 'simple-json'),
          column('sourceCode', 'code'),
        ],
      },
    });

    expect(target.enfyra_column.columns[0].type).toBe('enum');
    expect(target.enfyra_column.columns[0].options).toEqual([
      'string',
      'json',
      'objectId',
    ]);
    expect(target.example.columns).toEqual([
      { name: '_id', type: 'objectId', isPrimary: true },
      { name: 'title', type: 'string' },
      { name: 'enabled', type: 'bool' },
      { name: 'count', type: 'long' },
      { name: 'score', type: 'double' },
      { name: 'publishedAt', type: 'date' },
      { name: 'payload', type: 'json' },
      { name: 'sourceCode', type: 'code' },
    ]);
  });

  it('materializes the SQL target from the same declaration', () => {
    const target = toSqlTargetSnapshot({
      example: {
        name: 'example',
        columns: [
          column('id', 'int', { isPrimary: true }),
          column('title', 'varchar'),
          column('payload', 'simple-json'),
        ],
      },
    });

    expect(target.example.columns).toEqual([
      { name: 'id', type: 'int', isPrimary: true },
      { name: 'title', type: 'varchar' },
      { name: 'payload', type: 'simple-json' },
    ]);
  });

  it('projects Mongo migration targets from their own declarations', () => {
    const source = {
      tables: [
        {
          _unique: { name: { _eq: 'enfyra_file' } },
          columnsToModify: [
            {
              from: { name: 'isPublished' },
              to: {
                name: 'isPublic',
                sqlType: { type: 'boolean' },
                mongoType: { type: 'bool' },
              },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_method' } },
          columnsToModify: [
            {
              from: {
                name: 'method',
                sqlType: { type: 'varchar' },
                mongoType: { type: 'string' },
              },
              to: {
                name: 'name',
                sqlType: { type: 'varchar' },
                mongoType: { type: 'string' },
              },
            },
          ],
        },
      ],
    };

    const target = toMongoTargetMigration(source as any)!;

    expect(target.tables[0].columnsToModify?.[0]).toEqual({
      from: { name: 'isPublished' },
      to: { name: 'isPublic', type: 'bool' },
    });
    expect(target.tables[1].columnsToModify?.[0]).toEqual({
      from: { name: 'method', type: 'string' },
      to: { name: 'name', type: 'string' },
    });
    expect(source.tables[1].columnsToModify[0].to.mongoType.type).toBe('string');
  });

  it('projects the persisted type-picker target from its own backend declaration', () => {
    const source = {
      enfyra_column: [
        {
          _unique: {
            _and: [
              { table: { name: { _eq: 'enfyra_column' } } },
              { name: { _eq: 'type' } },
            ],
          },
          sqlType: { type: 'enum', options: ['varchar', 'simple-json', 'code'] },
          mongoType: { type: 'enum', options: ['string', 'json', 'code'] },
        },
        {
          _unique: {
            _and: [
              { table: { name: { _eq: 'example' } } },
              { name: { _eq: 'status' } },
            ],
          },
          sqlType: { type: 'enum', options: ['varchar', 'simple-json'] },
        },
      ],
    };

    const target = toMongoTargetDataMigration(source as any);

    expect(target.enfyra_column[0].options).toEqual(['string', 'json', 'code']);
    expect(target.enfyra_column[0].type).toBe('enum');
    expect(target.enfyra_column[1].options).toBeUndefined();
    expect(target.enfyra_column[1].type).toBeUndefined();
    expect(source.enfyra_column[0].sqlType.options).toEqual([
      'varchar',
      'simple-json',
      'code',
    ]);
    expect(source.enfyra_column[0].mongoType.options).toEqual([
      'string',
      'json',
      'code',
    ]);
  });

  it('accepts only backend-native primitives plus Enfyra semantic types', () => {
    expect(isSupportedMySqlColumnType('longtext')).toBe(true);
    expect(isSupportedPostgresColumnType('longtext')).toBe(false);
    expect(isSupportedMySqlColumnType('code')).toBe(true);
    expect(isSupportedPostgresColumnType('code')).toBe(true);
    expect(isSupportedMySqlColumnType('object')).toBe(false);
    expect(isSupportedPostgresColumnType('object')).toBe(false);

    expect(isSupportedMongoColumnType('object')).toBe(true);
    expect(isSupportedMongoColumnType('code')).toBe(true);
    expect(isSupportedMongoColumnType('simple-json')).toBe(false);
    expect(isSupportedMongoColumnType('varchar')).toBe(false);
  });

  it('lets a column declare its own Mongo type without leaking the declaration', () => {
    const target = toMongoTargetSnapshot({
      example: {
        name: 'example',
        columns: [
          {
            name: 'payload',
            sqlType: { type: 'simple-json' },
            mongoType: { type: 'array' },
          },
          {
            name: 'settings',
            sqlType: { type: 'simple-json' },
            mongoType: { type: 'object' },
          },
          column('fallback', 'simple-json'),
        ],
      },
    });

    expect(target.example.columns).toEqual([
      { name: 'payload', type: 'array' },
      { name: 'settings', type: 'object' },
      { name: 'fallback', type: 'json' },
    ]);
    for (const projected of target.example.columns) {
      expect('mongoType' in projected).toBe(false);
      expect('sqlType' in projected).toBe(false);
    }
  });

  it('materializes an enum column from its own backend declaration', () => {
    const target = toMongoTargetSnapshot({
      enfyra_column: {
        name: 'enfyra_column',
        columns: [
          {
            name: 'type',
            sqlType: { type: 'enum', options: ['varchar', 'simple-json'] },
            mongoType: {
              type: 'enum',
              options: ['string', 'json', 'objectId'],
            },
          },
        ],
      },
    });

    expect(target.enfyra_column.columns[0].type).toBe('enum');
    expect(target.enfyra_column.columns[0].options).toEqual([
      'string',
      'json',
      'objectId',
    ]);
  });
});
