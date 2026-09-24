import { QueryBuilderService } from '@enfyra/kernel';
import knexFactory from 'knex';

const metadata = {
  tables: new Map([
    ['enfyra_method', {
      name: 'enfyra_method',
      columns: [{ name: 'id', type: 'integer', isPrimary: true }, { name: 'name', type: 'text' }],
      relations: [{ propertyName: 'routes', type: 'one-to-many', targetTable: 'enfyra_route', foreignKeyColumn: 'methodId' }],
    }],
    ['enfyra_route', {
      name: 'enfyra_route',
      columns: [{ name: 'id', type: 'integer', isPrimary: true }],
      relations: [],
    }],
    ['enfyra_package', {
      name: 'enfyra_package',
      columns: [{ name: 'id', type: 'integer', isPrimary: true }, { name: 'name', type: 'text' }],
      relations: [],
    }],
  ]),
};

describe('SQL boot projections through the built Kernel', () => {
  it.each([
    ['postgres', 'pg', '"'],
    ['mysql', 'mysql2', '`'],
    ['sqlite', 'sqlite3', '"'],
  ])('quotes generated scalar selections exactly once on %s', async (dbType, client, quote) => {
    const knex = knexFactory({ client, useNullAsDefault: true });
    const statements: string[] = [];
    const originalRunner = knex.client.runner;
    knex.client.runner = ((builder: any) => ({
      run: async () => {
        statements.push(builder.toSQL().sql);
        return [];
      },
    })) as typeof originalRunner;
    const query = new QueryBuilderService({
      databaseConfigService: { getDbType: () => dbType, isMongoDb: () => false },
      knexService: { getKnex: () => knex, parseResult: async (rows: any[]) => rows },
      lazyRef: { metadataCacheService: { isLoaded: () => true, getMetadata: () => metadata } },
    });
    try {
      await query.find({ table: 'enfyra_method', fields: ['id', 'name'], limit: 0 });
      await query.find({ table: 'enfyra_package', fields: ['*'], limit: 0 });
      for (const sql of statements) expect(sql).not.toMatch(/\blimit\b/i);
      for (const sql of statements) expect(sql).not.toContain(quote.repeat(3));
      expect(statements[0]).toContain(`${quote}enfyra_method${quote}.${quote}name${quote}`);
      expect(statements[1]).toContain(`${quote}enfyra_package${quote}.${quote}name${quote}`);
    } finally {
      knex.client.runner = originalRunner;
      await knex.destroy();
    }
  });
});
