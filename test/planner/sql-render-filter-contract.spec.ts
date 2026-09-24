import knex from 'knex';
import { describe, expect, it } from 'vitest';
import { renderFilterToKnex, type FilterNode } from '@enfyra/kernel';

function compile(
  node: FilterNode,
  dbType: 'postgres' | 'mysql' | 'sqlite' = 'postgres',
) {
  const client = dbType === 'postgres' ? 'pg' : dbType === 'mysql' ? 'mysql2' : 'sqlite3';
  const db = knex({ client, useNullAsDefault: dbType === 'sqlite' });
  const query = db('users').select('*');
  renderFilterToKnex(query, node, { dbType, rootTable: 'users' });
  return query.toSQL();
}

function compare(fieldName: string, op: any, value: any): FilterNode {
  return {
    kind: 'compare',
    field: { joinId: null, fieldName },
    op,
    value,
  };
}

describe('SQL filter renderer contracts', () => {
  it('preserves boolean constants inside OR and NOT expressions', () => {
    expect(
      compile({ kind: 'or', children: [{ kind: 'true' }, compare('name', 'eq', 'x')] })
        .sql,
    ).toContain('1 = 1');
    expect(compile({ kind: 'not', child: { kind: 'true' } }).sql).toContain(
      'not (1 = 1)',
    );
  });

  it('fails closed when a join target reaches the root renderer', () => {
    expect(() =>
      compile({ kind: 'relation_exists', joinId: 'team', negate: false }),
    ).toThrow('relation_exists');
    expect(() =>
      compile({
        kind: 'compare',
        field: { joinId: 'team', fieldName: 'name' },
        op: 'eq',
        value: 'core',
      }),
    ).toThrow('join target');
  });

  it('uses SQL null and list semantics for comparisons', () => {
    expect(compile(compare('deletedAt', 'eq', null)).sql).toContain(
      '"users"."deletedAt" is null',
    );
    expect(compile(compare('deletedAt', 'neq', null)).sql).toContain(
      '"users"."deletedAt" is not null',
    );
    expect(compile(compare('name', 'in', [])).sql).toContain('1 = 0');
    expect(compile(compare('name', 'not_in', [])).sql).toContain('1 = 1');
    const nullableList = compile(compare('name', 'not_in', [null]));
    expect(nullableList.sql).toContain('not in (?)');
    expect(nullableList.bindings).toEqual([null]);
    expect(compile({ kind: 'not', child: compare('name', 'in', ['x', null]) }).bindings).toEqual(['x', null]);
  });

  it('rejects malformed ranges and unsafe qualified identifiers', () => {
    expect(() => compile(compare('name', 'between', ['a']))).toThrow(
      'Invalid between filter value',
    );
    expect(() => compile(compare('other.name', 'eq', 'x'))).toThrow(
      'Invalid SQL filter field',
    );
  });

  it('treats LIKE metacharacters literally without MySQL unaccent()', () => {
    const compiled = compile(
      compare('name', 'contains', '50%_off=now'),
      'mysql',
    );
    expect(compiled.sql).not.toContain('unaccent');
    expect(compiled.sql.toLowerCase()).toContain("escape '='");
    expect(compiled.bindings).toEqual(['50=%=_off==now']);
  });
});
