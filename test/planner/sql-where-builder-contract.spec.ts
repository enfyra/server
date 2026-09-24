import knex from 'knex';
import { describe, expect, it } from 'vitest';
import {
  applyWhereToKnex,
  buildSqlWherePartsFromFieldAst,
  buildWhereClause,
} from '@enfyra/kernel';

function metadata() {
  return {
    name: 'users',
    columns: [
      { name: 'id', type: 'uuid' },
      { name: 'name', type: 'varchar' },
      { name: 'deletedAt', type: 'timestamp' },
      { name: 'active', type: 'boolean' },
    ],
    relations: [],
  } as any;
}

function sqlFor(
  client: 'pg' | 'mysql2' | 'sqlite3',
  filter: any,
  dbType: 'postgres' | 'mysql' | 'sqlite',
) {
  const db = knex({ client, useNullAsDefault: client === 'sqlite3' });
  return buildWhereClause(
    db('users').select('*'),
    filter,
    'users',
    dbType,
    metadata(),
  ).toSQL();
}

describe('SQL where builder contracts', () => {
  it('uses IS NULL for scalar and operator null equality', () => {
    expect(sqlFor('pg', { deletedAt: null }, 'postgres').sql).toContain(
      '"users"."deletedAt" is null',
    );
    expect(
      sqlFor('pg', { deletedAt: { _eq: null } }, 'postgres').sql,
    ).toContain('"users"."deletedAt" is null');
    expect(
      sqlFor('pg', { deletedAt: { _neq: null } }, 'postgres').sql,
    ).toContain('"users"."deletedAt" is not null');
  });

  it('keeps _not inside an OR group', () => {
    const compiled = sqlFor(
      'pg',
      {
        _or: [
          { active: { _eq: true } },
          { _not: { name: { _eq: 'blocked' } } },
        ],
      },
      'postgres',
    );
    expect(compiled.sql).toMatch(/or \(not/);
  });

  it('rejects invalid UUIDs, qualifiers, and mixed operator objects', () => {
    expect(() =>
      sqlFor('pg', { id: { _eq: 'not-a-uuid' } }, 'postgres'),
    ).toThrow('Invalid UUID');
    expect(() =>
      sqlFor('pg', { 'other.name': { _eq: 'alice' } }, 'postgres'),
    ).toThrow('Invalid SQL filter field');
    expect(() =>
      sqlFor(
        'pg',
        { name: { _eq: 'alice', misspelledConstraint: 'ignored' } },
        'postgres',
      ),
    ).toThrow('Invalid SQL filter operator');
  });

  it('treats LIKE metacharacters literally and avoids MySQL unaccent()', () => {
    const compiled = sqlFor(
      'mysql2',
      { name: { _contains: '50%_off=now' } },
      'mysql',
    );
    expect(compiled.sql).not.toContain('unaccent');
    expect(compiled.sql.toLowerCase()).toContain("escape '='");
    expect(compiled.bindings).toEqual(['%50=%=_off==now%']);
  });

  it('renders raw null and empty-list predicates with SQL semantics', () => {
    expect(
      buildSqlWherePartsFromFieldAst(
        {
          deletedAt: { _eq: null },
          name: { _in: [] },
          active: { _not_in: [] },
        },
        'users',
        metadata(),
        'postgres',
      ),
    ).toEqual([
      '"users"."deletedAt" IS NULL',
      'FALSE',
      'TRUE',
    ]);
  });

  it('renders raw list predicates with SQL null-list semantics', () => {
    expect(
      buildSqlWherePartsFromFieldAst(
        {
          name: { _in: ['alice', null] },
          active: { _not_in: [true, null] },
        },
        'users',
        metadata(),
        'postgres',
      ),
    ).toEqual([
      `"users"."name" IN (E'alice', NULL)`,
      '"users"."active" NOT IN (true, NULL)',
    ]);
  });

  it('renders raw literal LIKE patterns without MySQL unaccent()', () => {
    const parts = buildSqlWherePartsFromFieldAst(
      { name: { _contains: `50%_off=now\\'` } },
      'users',
      metadata(),
      'mysql',
    );
    expect(parts[0]).not.toContain('unaccent');
    expect(parts[0]).toContain("ESCAPE '='");
    expect(parts[0]).toContain('CONVERT(X\'35303d253d5f6f66663d3d6e6f775c27\' USING utf8mb4)');
  });

  it('rejects malformed bound ranges', () => {
    const query = new Proxy(
      {},
      {
        get: () => () => query,
      },
    );
    expect(() =>
      applyWhereToKnex(
        query,
        [{ field: 'name', operator: '_between', value: ['a'] }],
        'users',
        { tables: new Map() },
        'postgres',
      ),
    ).toThrow('Invalid _between filter value');
  });

  it('rejects malformed raw ranges and invalid raw UUIDs', () => {
    expect(() =>
      buildSqlWherePartsFromFieldAst(
        { name: { _between: ['a'] } },
        'users',
        metadata(),
        'postgres',
      ),
    ).toThrow('Invalid _between filter value');
    expect(() =>
      buildSqlWherePartsFromFieldAst(
        { id: { _eq: 'not-a-uuid' } },
        'users',
        metadata(),
        'postgres',
      ),
    ).toThrow('Invalid UUID');
  });
});
