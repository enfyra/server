import knex from 'knex';
import { describe, expect, it } from 'vitest';
import { applyRelationFilters } from '@enfyra/kernel';

const db = knex({ client: 'pg' });

const users = {
  name: 'users',
  columns: [
    { name: 'id', type: 'uuid' },
    { name: 'status', type: 'varchar' },
    { name: 'teamId', type: 'uuid', isNullable: true },
  ],
  relations: [
    {
      propertyName: 'team',
      type: 'many-to-one' as const,
      targetTable: 'teams',
      foreignKeyColumn: 'teamId',
    },
  ],
};

const teams = {
  name: 'teams',
  columns: [
    { name: 'id', type: 'uuid' },
    { name: 'name', type: 'varchar' },
  ],
  relations: [],
};

const getMetadata = async (tableName: string) =>
  tableName === 'users' ? (users as any) : tableName === 'teams' ? (teams as any) : null;

async function sqlFor(filter: any) {
  const query = db('users').select('users.*');
  await applyRelationFilters(
    db,
    query,
    filter,
    'users',
    users as any,
    'postgres',
    getMetadata,
  );
  return query.toSQL();
}

describe('SQL relation filter contracts', () => {
  it('keeps sibling predicates beside _and inside an OR branch', async () => {
    const compiled = await sqlFor({
      _or: [
        {
          _and: [{ status: { _eq: 'active' } }],
          team: { name: { _eq: 'core' } },
        },
        { status: { _eq: 'pending' } },
      ],
    });
    expect(compiled.sql).toMatch(/from "teams" as "([^"]+)".*"\1"\."name"/);
    expect(compiled.bindings).toContain('core');
  });

  it('negates field and relation predicates as one conjunction', async () => {
    const compiled = await sqlFor({
      _not: {
        status: { _eq: 'active' },
        team: { name: { _eq: 'core' } },
      },
    });
    expect(compiled.sql).toContain('not (');
    expect(compiled.sql).toContain('"users"."status"');
    expect(compiled.sql).toContain('exists (select 1 from "teams"');
    expect(compiled.sql).not.toMatch(/not \([^)]*status[^)]*\) and not exists/s);
  });

  it('preserves SQL null semantics for direct foreign-key negation', async () => {
    const compiled = await sqlFor({
      _not: { team: { _eq: '11111111-1111-1111-1111-111111111111' } },
    });
    expect(compiled.sql).toContain('not (');
    expect(compiled.sql).toContain('"users"."teamId" =');
    expect(compiled.sql).not.toContain('"users"."teamId" !=');
  });

  it('handles empty and null relation ID lists without invalid SQL', async () => {
    const emptyIn = await sqlFor({ team: { _in: [] } });
    expect(emptyIn.sql).toContain('1 = 0');
    expect(emptyIn.sql).not.toContain('in ()');

    const emptyNotIn = await sqlFor({ team: { _not_in: [] } });
    expect(emptyNotIn.sql).toContain('1 = 1');
    expect(emptyNotIn.sql).not.toContain('not in ()');

    const nullNotIn = await sqlFor({ team: { _not_in: [null] } });
    expect(nullNotIn.sql).toContain('1 = 0');
  });

  it('handles null direct relation equality with IS NULL semantics', async () => {
    const equal = await sqlFor({ team: { _eq: null } });
    expect(equal.sql).toContain('"users"."teamId" is null');
    const unequal = await sqlFor({ team: { _neq: null } });
    expect(unequal.sql).toContain('"users"."teamId" is not null');
  });
});
