import knex from 'knex';
import { describe, expect, it } from 'vitest';
import { SqlBatchAdapter } from '@enfyra/kernel';

function metadata() {
  return {
    tables: new Map([
      [
        'posts',
        {
          name: 'posts',
          columns: [
            { name: 'id', type: 'uuid', isPrimary: true },
            { name: 'authorId', type: 'uuid' },
            { name: 'teamId', type: 'uuid' },
          ],
          relations: [
            {
              propertyName: 'a_b',
              type: 'many-to-one',
              targetTableName: 'teams',
              foreignKeyColumn: 'teamId',
            },
            {
              propertyName: 'a',
              type: 'many-to-one',
              targetTableName: 'teams',
              foreignKeyColumn: 'teamId',
            },
          ],
        },
      ],
      [
        'teams',
        {
          name: 'teams',
          columns: [
            { name: 'id', type: 'uuid', isPrimary: true },
            { name: 'b_c', type: 'varchar' },
            { name: 'c', type: 'varchar' },
          ],
          relations: [
            {
              propertyName: 'b',
              type: 'many-to-one',
              targetTableName: 'teams',
              foreignKeyColumn: 'id',
            },
          ],
        },
      ],
    ]),
  };
}

class CapturingAdapter extends SqlBatchAdapter {
  queries: any[] = [];

  protected override runQuery<T>(query: Promise<T>): Promise<T> {
    this.queries.push(query);
    return Promise.resolve([] as T);
  }
}

describe('SQL batch adapter contracts', () => {
  it('uses type-aware identity keys', () => {
    const adapter = new SqlBatchAdapter(knex({ client: 'pg' }), 'postgres');
    expect(adapter.keyOf(1)).not.toBe(adapter.keyOf('1'));
    expect(adapter.keyOf(null)).not.toBe(adapter.keyOf(''));
    expect(adapter.keyOf(false)).not.toBe(adapter.keyOf('false'));
    expect(adapter.keyOf(1n)).not.toBe(adapter.keyOf(1));
  });

  it('uses injective aliases for dotted sort paths', async () => {
    const adapter = new CapturingAdapter(
      knex({ client: 'pg' }),
      'postgres',
      metadata(),
    );
    await adapter.fetchOwner(
      'posts',
      ['post-1'],
      { selectCols: ['id'], pkCol: 'id' },
      {
        relationName: 'posts',
        type: 'many-to-one',
        targetTable: 'posts',
        fields: ['id'],
        userSort: ['a_b.c', 'a.b.c'],
      },
    );
    const sql = adapter.queries[0].toSQL().sql;
    const aliases = [...sql.matchAll(/left join "teams" as "([^"]+)"/g)].map(
      (match) => match[1],
    );
    expect(new Set(aliases).size).toBe(aliases.length);
    expect(aliases).toHaveLength(3);
    expect(aliases).toContain('__sort_3_a_b');
    expect(aliases).toContain('__sort_1_a_1_b');
  });

  it('qualifies owner and inverse scopes when sort joins are present', async () => {
    const owner = new CapturingAdapter(
      knex({ client: 'pg' }),
      'postgres',
      metadata(),
    );
    await owner.fetchOwner(
      'posts',
      ['post-1'],
      { selectCols: ['id'], pkCol: 'id' },
      {
        relationName: 'posts',
        type: 'many-to-one',
        targetTable: 'posts',
        fields: ['id'],
        userSort: 'a.c',
      },
    );
    expect(owner.queries[0].toSQL().sql).toContain(
      'where "posts"."id" in',
    );

    const inverse = new CapturingAdapter(
      knex({ client: 'pg' }),
      'postgres',
      metadata(),
    );
    await inverse.fetchInverse(
      'posts',
      'authorId',
      ['user-1'],
      { selectCols: ['id'], pkCol: 'id' },
      {
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id'],
        userSort: 'a.c',
      },
    );
    const inverseSql = inverse.queries[0].toSQL().sql;
    expect(inverseSql).toContain('where "posts"."authorId" in');
    expect(inverseSql).toContain('order by');
  });

  it('does not invent an inverse foreign key from the target table name', () => {
    const adapter = new SqlBatchAdapter(knex({ client: 'pg' }), 'postgres');
    expect(() =>
      adapter.resolveInverseFkField({
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['id'],
      }),
    ).toThrow('inverse foreign key');
  });
});
