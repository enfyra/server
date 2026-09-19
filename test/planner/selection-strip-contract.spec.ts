import { describe, expect, it } from 'vitest';
import { QueryPlanner, SqlQueryExecutor, validateDeepOptions } from '@enfyra/kernel';
import knexFactory from 'knex';

const metadata = { tables: new Map([
  ['routes', { name: 'routes', columns: [{ name: 'id', type: 'integer', isPrimary: true }, { name: 'name', type: 'text' }, { name: 'mainTableId', type: 'integer' }], relations: [{ propertyName: 'mainTable', type: 'many-to-one', targetTable: 'tables', foreignKeyColumn: 'mainTableId' }] }],
  ['tables', { name: 'tables', columns: [{ name: 'id', type: 'integer', isPrimary: true }, { name: 'name', type: 'text' }], relations: [] }],
]) };

describe('selected fields strip unknown metadata paths', () => {
  it('strips missing root and nested fields while keeping valid selections', () => {
    const plan = new QueryPlanner().plan({ tableName: 'routes', fields: ['name', 'missing', 'methods.*', 'mainTable.name', 'mainTable.icon'], metadata, dbType: 'postgres' });
    expect(plan.rawFields).toEqual(['name', 'mainTable.name']);
  });
  it('strips missing deep fields without weakening filter or sort validation', () => {
    expect(() => validateDeepOptions('routes', { mainTable: { fields: ['name', 'icon'] } }, metadata)).not.toThrow();
    expect(() => new QueryPlanner().plan({ tableName: 'routes', filter: { missing: { _eq: 1 } }, metadata, dbType: 'postgres' })).toThrow();
    expect(() => new QueryPlanner().plan({ tableName: 'routes', sort: 'missing', metadata, dbType: 'postgres' })).toThrow();
  });
  it('never expands an unknown-only selection into all scalar fields', async () => {
    const knex = knexFactory({ client: 'pg' });
    const statements: string[] = [];
    knex.client.runner = ((builder: any) => ({ run: async () => { statements.push(builder.toSQL().sql); return []; } })) as any;
    try {
      await new SqlQueryExecutor(knex, 'postgres').execute({ tableName: 'tables', fields: ['missing'], metadata });
      expect(statements).toHaveLength(1);
      expect(statements[0]).not.toMatch(/select \*/i);
      expect(statements[0]).not.toContain('missing');
      expect(statements[0]).not.toContain('"name"');
    } finally { await knex.destroy(); }
  });
});
